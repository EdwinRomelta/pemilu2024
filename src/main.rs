use std::collections::HashMap;
use std::env;

use mongodb::Client;
use mongodb::options::{ClientOptions, ResolverConfig};
use reqwest::Error;
use reqwest_middleware::{ClientBuilder, ClientWithMiddleware};
use reqwest_retry;
use reqwest_retry::{policies::ExponentialBackoff, RetryTransientMiddleware};
use serde::{Deserialize, Serialize};

const MAX_PROCESS_SIZE: usize = 100;
const MAX_BATCH_SIZE: usize = 1000;

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
    nama: String,
    id: i32,
    kode: String,
    tingkat: i8,
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

    let vote_results1 = process_region_1(client.clone(), request_client.clone()).await;

    let vote_results2 =
        process_region_2(vote_results1, client.clone(), request_client.clone()).await;

    let mut vote_results3: Vec<VoteResult> = Vec::new();
    for element in vote_results2
        .chunks(MAX_PROCESS_SIZE)
        .map(|(batch)| batch.to_vec())
        .collect::<Vec<Vec<VoteResult>>>()
    {
        let mut result = process_region_3(element, client.clone(), request_client.clone()).await;
        vote_results3.append(&mut result);
    }

    let mut vote_results4: Vec<VoteResult> = Vec::new();
    for element in vote_results3
        .chunks(MAX_PROCESS_SIZE)
        .map(|(batch)| batch.to_vec())
        .collect::<Vec<Vec<VoteResult>>>()
    {
        let mut result = process_region_4(element, client.clone(), request_client.clone()).await;
        vote_results4.append(&mut result);
    }

    for element in vote_results4
        .chunks(MAX_PROCESS_SIZE)
        .map(|(batch)| batch.to_vec())
        .collect::<Vec<Vec<VoteResult>>>()
    {
        process_region_5(element, client.clone(), request_client.clone()).await;
    }

    Ok(())
}

async fn process_region_1(client: Client, request_client: ClientWithMiddleware) -> Vec<VoteResult> {
    let region1 = populate_region_1(request_client.clone()).await;
    let vote1 = vote_count_region_1(request_client.clone()).await;
    let vote_results1 = join_vote(region1.clone(), vote1);
    insert_vote("Tingkat 1", vote_results1.clone(), client).await;

    vote_results1
}

async fn process_region_2(
    vote_results1: Vec<VoteResult>,
    client: Client,
    request_client: ClientWithMiddleware,
) -> Vec<VoteResult> {
    let region2_tasks: Vec<_> = vote_results1
        .into_iter()
        .map(|vote_results| {
            let request_client = request_client.clone();
            tokio::spawn(async move {
                let region = populate_region_2(request_client.clone(), vote_results.clone()).await;
                let vote = vote_count_region_2(request_client.clone(), vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results2 = futures::future::join_all(region2_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();
    insert_vote("Tingkat 2", vote_results2.clone(), client).await;

    vote_results2
}

async fn process_region_3(
    vote_results2: Vec<VoteResult>,
    client: Client,
    request_client: ClientWithMiddleware,
) -> Vec<VoteResult> {
    let region3_tasks: Vec<_> = vote_results2
        .into_iter()
        .map(|vote_results| {
            let request_client = request_client.clone();
            tokio::spawn(async move {
                let region = populate_region_3(request_client.clone(), vote_results.clone()).await;
                let vote = vote_count_region_3(request_client.clone(), vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results3 = futures::future::join_all(region3_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();
    insert_vote("Tingkat 3", vote_results3.clone(), client).await;
    vote_results3
}

async fn process_region_4(
    vote_results3: Vec<VoteResult>,
    client: Client,
    request_client: ClientWithMiddleware,
) -> Vec<VoteResult> {
    let region4_tasks: Vec<_> = vote_results3
        .into_iter()
        .map(|vote_results| {
            let request_client = request_client.clone();
            tokio::spawn(async move {
                let region = populate_region_4(request_client.clone(), vote_results.clone()).await;
                let vote = vote_count_region_4(request_client.clone(), vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results4 = futures::future::join_all(region4_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();
    println!("Inserting Tingkat 4 with {} records", vote_results4.len());
    insert_vote("Tingkat 4", vote_results4.clone(), client).await;
    vote_results4
}

async fn process_region_5(
    vote_results4: Vec<VoteResult>,
    client: Client,
    request_client: ClientWithMiddleware,
) -> Vec<VoteResult> {
    let region5_tasks: Vec<_> = vote_results4
        .into_iter()
        .map(|vote_results| {
            let request_client = request_client.clone();
            tokio::spawn(async move {
                let region = populate_region_5(request_client.clone(), vote_results.clone()).await;
                let vote = vote_count_region_5(request_client.clone(), vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results5 = futures::future::join_all(region5_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();
    insert_vote("Tingkat 5", vote_results5.clone(), client).await;
    vote_results5
}

async fn populate_region_1(request_client: ClientWithMiddleware) -> Vec<Region> {
    let url = "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/0.json";
    let response = request_client
        .get(url)
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions: Vec<Region> = response.json().await.unwrap();
    regions
}

async fn populate_region_2(
    request_client: ClientWithMiddleware,
    vote_result1: VoteResult,
) -> Vec<Region> {
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}.json",
        vote_result1.kode
    );
    let response = request_client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions2 = response.json().await.unwrap();
    regions2
}

async fn populate_region_3(
    request_client: ClientWithMiddleware,
    vote_result2: VoteResult,
) -> Vec<Region> {
    let region2_code = &vote_result2.kode[0..2];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}.json",
        region2_code, vote_result2.kode
    );
    let response = request_client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions2 = response.json().await.unwrap();
    regions2
}

async fn populate_region_4(
    request_client: ClientWithMiddleware,
    vote_result3: VoteResult,
) -> Vec<Region> {
    let region2_code = &vote_result3.kode[0..2];
    let region3_code = &vote_result3.kode[0..4];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}.json",
        region2_code, region3_code, vote_result3.kode
    );
    let response = request_client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions3 = response.json().await.unwrap();
    regions3
}

async fn populate_region_5(
    request_client: ClientWithMiddleware,
    vote_result4: VoteResult,
) -> Vec<Region> {
    let region2_code = &vote_result4.kode[0..2];
    let region3_code = &vote_result4.kode[0..4];
    let region4_code = &vote_result4.kode[0..6];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}/{}.json",
        region2_code, region3_code, region4_code, vote_result4.kode
    );
    let response = request_client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions5 = response.json().await.unwrap();
    regions5
}

async fn vote_count_region_1(request_client: ClientWithMiddleware) -> Vote {
    let url = "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp.json";
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

async fn vote_count_region_2(
    request_client: ClientWithMiddleware,
    vote_result1: VoteResult,
) -> Vote {
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}.json",
        vote_result1.kode
    );
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

async fn vote_count_region_3(
    request_client: ClientWithMiddleware,
    vote_result2: VoteResult,
) -> Vote {
    let region2_code = &vote_result2.kode[0..2];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}.json",
        region2_code, vote_result2.kode
    );
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

async fn vote_count_region_4(
    request_client: ClientWithMiddleware,
    vote_result2: VoteResult,
) -> Vote {
    let region2_code = &vote_result2.kode[0..2];
    let region3_code = &vote_result2.kode[0..4];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}/{}.json",
        region2_code, region3_code, vote_result2.kode
    );
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

async fn vote_count_region_5(
    request_client: ClientWithMiddleware,
    vote_result4: VoteResult,
) -> Vote {
    let region2_code = &vote_result4.kode[0..2];
    let region3_code = &vote_result4.kode[0..4];
    let region4_code = &vote_result4.kode[0..6];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}/{}/{}.json",
        region2_code, region3_code, region4_code, vote_result4.kode
    );
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

fn join_vote(region: Vec<Region>, vote: Vote) -> Vec<VoteResult> {
    region
        .into_iter()
        .map(|region| {
            let vote = vote.table.get(&region.kode);
            match vote {
                Some(vote) => VoteResult {
                    nama: region.nama,
                    id: region.id,
                    kode: region.kode,
                    tingkat: region.tingkat,
                    i01: vote.i01.unwrap_or_else(|| 0),
                    i02: vote.i02.unwrap_or_else(|| 0),
                    i03: vote.i03.unwrap_or_else(|| 0),
                },
                None => VoteResult {
                    nama: region.nama,
                    id: region.id,
                    kode: region.kode,
                    tingkat: region.tingkat,
                    i01: 0,
                    i02: 0,
                    i03: 0,
                },
            }
        })
        .collect::<Vec<VoteResult>>()
}

async fn insert_vote(title: &str, vote_results: Vec<VoteResult>, client: Client) {
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
