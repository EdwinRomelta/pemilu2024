use std::collections::HashMap;

use reqwest::Error;
use serde::{Deserialize, Serialize};

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

#[derive(Debug, Clone)]
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
    let region1 = populate_region_1().await;
    let vote1 = vote_count_region_1().await;
    let vote_results1 = join_vote(region1.clone(), vote1);

    let region2_tasks: Vec<_> = vote_results1
        .into_iter()
        .map(|vote_results| {
            tokio::spawn(async {
                let region = populate_region_2(vote_results.clone()).await;
                let vote = vote_count_region_2(vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results2 = futures::future::join_all(region2_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();

    let region3_tasks: Vec<_> = vote_results2
        .into_iter()
        .map(|vote_results| {
            tokio::spawn(async {
                let region = populate_region_3(vote_results.clone()).await;
                let vote = vote_count_region_3(vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results3 = futures::future::join_all(region3_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();

    let region4_tasks: Vec<_> = vote_results3
        .into_iter()
        .map(|vote_results| {
            tokio::spawn(async {
                let region = populate_region_4(vote_results.clone()).await;
                let vote = vote_count_region_4(vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results4 = futures::future::join_all(region4_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();

    let region5_tasks: Vec<_> = vote_results4
        .into_iter()
        .map(|vote_results| {
            tokio::spawn(async {
                let region = populate_region_5(vote_results.clone()).await;
                let vote = vote_count_region_5(vote_results).await;
                join_vote(region, vote)
            })
        })
        .collect();
    let vote_results5 = futures::future::join_all(region5_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<VoteResult>>();
    Ok(())
}

async fn populate_region_1() -> Vec<Region> {
    let url = "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/0.json";
    let client = reqwest::Client::new();
    let response = client
        .get(url)
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions: Vec<Region> = response.json().await.unwrap();
    regions
}

async fn populate_region_2(vote_result1: VoteResult) -> Vec<Region> {
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}.json",
        vote_result1.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions2 = response.json().await.unwrap();
    regions2
}

async fn populate_region_3(vote_result2: VoteResult) -> Vec<Region> {
    let region2_code = &vote_result2.kode[0..2];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}.json",
        region2_code, vote_result2.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions2 = response.json().await.unwrap();
    regions2
}

async fn populate_region_4(vote_result3: VoteResult) -> Vec<Region> {
    let region2_code = &vote_result3.kode[0..2];
    let region3_code = &vote_result3.kode[0..4];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}.json",
        region2_code, region3_code, vote_result3.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions3 = response.json().await.unwrap();
    regions3
}

async fn populate_region_5(vote_result4: VoteResult) -> Vec<Region> {
    let region2_code = &vote_result4.kode[0..2];
    let region3_code = &vote_result4.kode[0..4];
    let region4_code = &vote_result4.kode[0..6];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}/{}..json",
        region2_code, region3_code, region4_code, vote_result4.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let regions5 = response.json().await.unwrap();
    regions5
}

async fn vote_count_region_1() -> Vote {
    let url = "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp.json";
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let vote: Vote = response.json().await.unwrap();
    vote
}

async fn vote_count_region_2(vote_result1: VoteResult) -> Vote {
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}.json",
        vote_result1.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let vote: Vote = response.json().await.unwrap();
    vote
}

async fn vote_count_region_3(vote_result2: VoteResult) -> Vote {
    let region2_code = &vote_result2.kode[0..2];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}.json",
        region2_code, vote_result2.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let vote: Vote = response.json().await.unwrap();
    vote
}

async fn vote_count_region_4(vote_result2: VoteResult) -> Vote {
    let region2_code = &vote_result2.kode[0..2];
    let region3_code = &vote_result2.kode[0..4];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}/{}.json",
        region2_code, region3_code, vote_result2.kode
    );
    let client = reqwest::Client::new();
    let response = client
        .get(url.clone())
        .header("Content-Type", "application/json")
        .send()
        .await
        .unwrap();
    println!("Status Code {} : {}", url, response.status());
    let vote: Vote = response.json().await.unwrap();
    vote
}

async fn vote_count_region_5(vote_result4: VoteResult) -> Vote {
    let region2_code = &vote_result4.kode[0..2];
    let region3_code = &vote_result4.kode[0..4];
    let region4_code = &vote_result4.kode[0..6];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/pemilu/hhcw/ppwp/{}/{}/{}/{}.json",
        region2_code, region3_code, region4_code, vote_result4.kode
    );
    let client = reqwest::Client::new();
    let response = client
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    populate_vote().await.unwrap();
    Ok(())
}
