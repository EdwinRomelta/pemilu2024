use std::{env, fmt};
use std::collections::HashMap;

use mongodb::Client;
use mongodb::options::{ClientOptions, ResolverConfig};
use reqwest::Error;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::{Deserialize, Serialize};

const MAX_PROCESS_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 1000;

#[derive(Clone, Copy, Debug)]
enum Level {
    L1,
    L2,
    L3,
    L4,
    L5,
}

impl fmt::Display for Level {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{:?}", self)
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Region {
    nama: String,
    id: i32,
    kode: String,
    tingkat: i8,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Vote {
    table: HashMap<String, Count>,
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct Count {
    #[serde(rename = "100025")]
    i01: Option<i32>,
    #[serde(rename = "100026")]
    i02: Option<i32>,
    #[serde(rename = "100027")]
    i03: Option<i32>,
    psu: String,
    persen: Option<f32>,
    status_progress: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct VoteResult {
    #[serde(rename = "_id")]
    kode: String,
    tingkat: i8,
    #[serde(skip_serializing_if = "Option::is_none")]
    name_tingkat_1: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name_tingkat_2: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name_tingkat_3: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name_tingkat_4: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    name_tingkat_5: Option<String>,
    i01: i32,
    i02: i32,
    i03: i32,
}

async fn populate_vote() -> Result<(), Error> {
    let client_uri =
        env::var("MONGODB_URI").expect("You must set the MONGODB_URI environment var!");

    let options =
        ClientOptions::parse_with_resolver_config(client_uri, ResolverConfig::cloudflare())
            .await
            .unwrap();
    let client = Client::with_options(options).unwrap();

    let reqwest_client = reqwest::Client::builder().build().unwrap();
    let request_client = ClientBuilder::new(reqwest_client)
        .with(RetryTransientMiddleware::new_with_policy(
            ExponentialBackoff::builder()
                .retry_bounds(
                    std::time::Duration::from_millis(3000),
                    std::time::Duration::from_millis(10000),
                )
                .build_with_max_retries(3),
        ))
        .build();

    let vote_results1 =
        process_region(Level::L1, None, client.clone(), request_client.clone()).await;

    let vote_results2 = process_region(
        Level::L2,
        Some(vote_results1),
        client.clone(),
        request_client.clone(),
    )
        .await;

    let vote_results3 = batch_process_region(
        Level::L3,
        vote_results2,
        client.clone(),
        request_client.clone(),
    )
        .await;

    let vote_results4 = batch_process_region(
        Level::L4,
        vote_results3,
        client.clone(),
        request_client.clone(),
    )
        .await;

    batch_process_region(
        Level::L5,
        vote_results4,
        client.clone(),
        request_client.clone(),
    )
        .await;

    Ok(())
}

async fn process_region(
    level: Level,
    vote_results_option: Option<Vec<VoteResult>>,
    client: Client,
    request_client: ClientWithMiddleware,
) -> Vec<VoteResult> {
    let result = match level {
        Level::L1 => {
            let region1 = populate_region(Level::L1, request_client.clone(), None).await;
            let vote1 = vote_count_region(Level::L1, request_client.clone(), None).await;
            return join_vote(region1.clone(), vote1, None, 1);
        }
        _ => {
            let vote_results = vote_results_option.unwrap();
            let region2_tasks: Vec<_> = vote_results
                .into_iter()
                .map(|vote_results| {
                    let request_client = request_client.clone();
                    tokio::spawn(async move {
                        let region = populate_region(
                            level,
                            request_client.clone(),
                            Some(vote_results.clone()),
                        )
                            .await;
                        let vote = vote_count_region(
                            level,
                            request_client.clone(),
                            Some(vote_results.clone()),
                        )
                            .await;
                        join_vote(region, vote, Some(vote_results), 2)
                    })
                })
                .collect();
            futures::future::join_all(region2_tasks)
                .await
                .into_iter()
                .flat_map(|regions| regions.unwrap())
                .collect::<Vec<VoteResult>>()
        }
    };
    insert_vote(
        format!("Level {}", level.to_string()),
        result.clone(),
        client,
    )
        .await;
    result
}

async fn batch_process_region(
    level: Level,
    vote_results: Vec<VoteResult>,
    client: Client,
    request_client: ClientWithMiddleware,
) -> Vec<VoteResult> {
    let mut process_result: Vec<VoteResult> = Vec::new();
    for element in vote_results
        .chunks(MAX_PROCESS_SIZE)
        .map(|batch| batch.to_vec())
        .collect::<Vec<Vec<VoteResult>>>()
    {
        let mut result =
            process_region(level, Some(element), client.clone(), request_client.clone()).await;
        process_result.append(&mut result);
    }
    process_result
}

async fn populate_region(
    level: Level,
    request_client: ClientWithMiddleware,
    vote_result_option: Option<VoteResult>,
) -> Vec<Region> {
    let url = match level {
        Level::L1 => "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/0.json".to_string(),
        Level::L2 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}.json",
                vote_result.kode
            )
        }
        Level::L3 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}.json",
                &vote_result.kode[0..2],
                vote_result.kode
            )
        }
        Level::L4 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}.json",
                &vote_result.kode[0..2],
                &vote_result.kode[0..4],
                vote_result.kode
            )
        }
        Level::L5 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}/{}.json",
                &vote_result.kode[0..2],
                &vote_result.kode[0..4],
                &vote_result.kode[0..6],
                vote_result.kode
            )
        }
    };
    let response = request_client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions: Vec<Region> = response.json().await.unwrap();
    regions
}

async fn vote_count_region(
    level: Level,
    request_client: ClientWithMiddleware,
    vote_result_option: Option<VoteResult>,
) -> Vote {
    let url = match level {
        Level::L1 => "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp.json".to_string(),
        Level::L2 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}.json",
                vote_result.kode
            )
        }
        Level::L3 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}.json",
                &vote_result.kode[0..2],
                vote_result.kode
            )
        }
        Level::L4 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}/{}.json",
                &vote_result.kode[0..2],
                &vote_result.kode[0..4],
                vote_result.kode
            )
        }
        Level::L5 => {
            let vote_result = vote_result_option.unwrap();
            format!(
                "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}/{}/{}.json",
                &vote_result.kode[0..2],
                &vote_result.kode[0..4],
                &vote_result.kode[0..6],
                vote_result.kode
            )
        }
    };
    let response = request_client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let vote: Vote = response.json().await.unwrap();
    vote
}

fn join_vote(
    region: Vec<Region>,
    vote: Vote,
    vote_result: Option<VoteResult>,
    tingkat: i8,
) -> Vec<VoteResult> {
    region
        .into_iter()
        .map(|region| {
            let vote = vote.table.get(&region.kode);
            let name_tingkat_1 = if tingkat == 1 {
                Some(region.nama.clone())
            } else {
                match vote_result.clone() {
                    Some(value) => value.name_tingkat_1,
                    None => None,
                }
            };
            let name_tingkat_2 = if tingkat == 2 {
                Some(region.nama.clone())
            } else {
                match vote_result.clone() {
                    Some(value) => value.name_tingkat_2,
                    None => None,
                }
            };
            let name_tingkat_3 = if tingkat == 3 {
                Some(region.nama.clone())
            } else {
                match vote_result.clone() {
                    Some(value) => value.name_tingkat_3,
                    None => None,
                }
            };
            let name_tingkat_4 = if tingkat == 4 {
                Some(region.nama.clone())
            } else {
                match vote_result.clone() {
                    Some(value) => value.name_tingkat_4,
                    None => None,
                }
            };
            let name_tingkat_5 = if tingkat == 5 {
                Some(region.nama.clone())
            } else {
                match vote_result.clone() {
                    Some(value) => value.name_tingkat_5,
                    None => None,
                }
            };
            match vote {
                Some(vote) => VoteResult {
                    kode: region.kode,
                    tingkat: region.tingkat,
                    i01: vote.i01.unwrap_or_else(|| 0),
                    i02: vote.i02.unwrap_or_else(|| 0),
                    i03: vote.i03.unwrap_or_else(|| 0),
                    name_tingkat_1,
                    name_tingkat_2,
                    name_tingkat_3,
                    name_tingkat_4,
                    name_tingkat_5,
                },
                None => VoteResult {
                    kode: region.kode,
                    tingkat: region.tingkat,
                    i01: 0,
                    i02: 0,
                    i03: 0,
                    name_tingkat_1,
                    name_tingkat_2,
                    name_tingkat_3,
                    name_tingkat_4,
                    name_tingkat_5,
                },
            }
        })
        .collect::<Vec<VoteResult>>()
}

async fn insert_vote(title: String, vote_results: Vec<VoteResult>, client: Client) {
    let length = (vote_results.len() / MAX_BATCH_SIZE) + 1;
    let insert_task_1: Vec<_> = vote_results
        .clone()
        .chunks(MAX_BATCH_SIZE)
        .enumerate()
        .map(|(index, batch)| {
            let client = client.clone();
            let batch = batch.to_vec();
            let title = title.to_string();
            tokio::spawn(async move {
                println!(
                    "Inserting {} with {} records part {}/{}",
                    title,
                    batch.len(),
                    index + 1,
                    length
                );
                client
                    .database("pemilu")
                    .collection("vote")
                    .insert_many(batch, None)
                    .await
            })
        })
        .collect();
    futures::future::join_all(insert_task_1).await;
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    populate_vote().await.unwrap();
    Ok(())
}
