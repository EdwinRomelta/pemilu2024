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

async fn populate_vote() -> Result<(), Error> {
    let region1 = populate_region_1().await;

    let region2_tasks: Vec<_> = region1
        .into_iter()
        .map(|region| tokio::spawn(async { populate_region_2(region).await }))
        .collect();
    let region2 = futures::future::join_all(region2_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<Region>>();

    let region3_tasks: Vec<_> = region2
        .into_iter()
        .map(|region| tokio::spawn(async { populate_region_3(region).await }))
        .collect();
    let region3 = futures::future::join_all(region3_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<Region>>();

    let region4_tasks: Vec<_> = region3
        .into_iter()
        .map(|region| tokio::spawn(async { populate_region_4(region).await }))
        .collect();
    let region4 = futures::future::join_all(region4_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<Region>>();

    let region5_tasks: Vec<_> = region4
        .into_iter()
        .map(|region| tokio::spawn(async { populate_region_5(region).await }))
        .collect();
    let region5 = futures::future::join_all(region5_tasks)
        .await
        .into_iter()
        .flat_map(|regions| regions.unwrap())
        .collect::<Vec<Region>>();
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

async fn populate_region_2(region1: Region) -> Vec<Region> {
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}.json",
        region1.kode
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

async fn populate_region_3(region2: Region) -> Vec<Region> {
    let region2_code = &region2.kode[0..2];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}.json",
        region2_code, region2.kode
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

async fn populate_region_4(region3: Region) -> Vec<Region> {
    let region2_code = &region3.kode[0..2];
    let region3_code = &region3.kode[0..4];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}.json",
        region2_code, region3_code, region3.kode
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

async fn populate_region_5(region4: Region) -> Vec<Region> {
    let region2_code = &region4.kode[0..2];
    let region3_code = &region4.kode[0..4];
    let region4_code = &region4.kode[0..6];
    let url = format!(
        "https://sirekap-obj-data.kpu.go.id/wilayah/pemilu/ppwp/{}/{}/{}/{}..json",
        region2_code, region3_code, region4_code, region4.kode
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

#[tokio::main]
async fn main() -> Result<(), Error> {
    populate_vote().await.unwrap();
    Ok(())
}
