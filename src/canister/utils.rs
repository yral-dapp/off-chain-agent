use candid::encode_args;
use ic_agent::{export::Principal, Agent};
use std::{collections::HashMap, io::Error};

use crate::consts::PLATFORM_ORCHESTRATOR_ID;

pub async fn get_canisters_list(agent: &Agent) -> Result<Vec<Principal>, Error> {
    // Platform Orchestrator canister id
    let pf_o10r_canister_id = Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();

    // Get the list of subnet orchestrator canister ids

    let response = match agent
        .query(&pf_o10r_canister_id, "get_all_subnet_orchestrators")
        .with_arg(encode_args(()).unwrap())
        .call()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            println!("Unable to call the method, error: {:?}", err);
            return Err(Error::new(std::io::ErrorKind::Other, err));
        }
    };

    let subnet_o10r_ids = match candid::decode_one(&response) {
        Ok(result) => {
            let result: Vec<Principal> = result;
            result
        }
        Err(err) => {
            println!("Unable to decode the response, error: {:?}", err);
            return Err(Error::new(std::io::ErrorKind::Other, err));
        }
    };
    println!(
        "subnet_o10r_ids {:?}",
        subnet_o10r_ids
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
    );

    let mut canister_ids_list = vec![];

    // Iterate over the subnet orchestrator canister ids
    for subnet_o10r_canister_id in subnet_o10r_ids {
        // Get individual canister list

        let response = match agent
            .query(&subnet_o10r_canister_id, "get_user_canister_list")
            .with_arg(encode_args(()).unwrap())
            .call()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                println!("Unable to call the method, error: {:?}", err);
                return Err(Error::new(std::io::ErrorKind::Other, err));
            }
        };

        let canister_ids = match candid::decode_one(&response) {
            Ok(result) => {
                let result: Vec<Principal> = result;
                result
            }
            Err(err) => {
                println!("Unable to decode the response, error: {:?}", err);
                return Err(Error::new(std::io::ErrorKind::Other, err));
            }
        };
        println!(
            "subnet_o10r_canister_id {} canister_ids_len {:?}",
            subnet_o10r_canister_id,
            canister_ids.len()
        );

        canister_ids_list.extend(canister_ids);
    }

    Ok(canister_ids_list)
}

pub async fn get_canisters_list_all(agent: &Agent) -> Result<Vec<Principal>, Error> {
    // Platform Orchestrator canister id
    let pf_o10r_canister_id = Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();

    // Get the list of subnet orchestrator canister ids

    let response = match agent
        .query(&pf_o10r_canister_id, "get_all_subnet_orchestrators")
        .with_arg(encode_args(()).unwrap())
        .call()
        .await
    {
        Ok(response) => response,
        Err(err) => {
            println!("Unable to call the method, error: {:?}", err);
            return Err(Error::new(std::io::ErrorKind::Other, err));
        }
    };

    let subnet_o10r_ids = match candid::decode_one(&response) {
        Ok(result) => {
            let result: Vec<Principal> = result;
            result
        }
        Err(err) => {
            println!("Unable to decode the response, error: {:?}", err);
            return Err(Error::new(std::io::ErrorKind::Other, err));
        }
    };
    println!(
        "subnet_o10r_ids {:?}",
        subnet_o10r_ids
            .iter()
            .map(|x| x.to_string())
            .collect::<Vec<String>>()
    );

    let mut canister_ids_list = vec![];

    // Iterate over the subnet orchestrator canister ids
    for subnet_o10r_canister_id in &subnet_o10r_ids {
        // Get individual canister list

        let response = match agent
            .query(subnet_o10r_canister_id, "get_user_canister_incl_avail_list")
            .with_arg(encode_args(()).unwrap())
            .call()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                println!("Unable to call the method, error: {:?}", err);
                return Err(Error::new(std::io::ErrorKind::Other, err));
            }
        };

        let canister_ids = match candid::decode_one(&response) {
            Ok(result) => {
                let result: Vec<Principal> = result;
                result
            }
            Err(err) => {
                println!("Unable to decode the response, error: {:?}", err);
                return Err(Error::new(std::io::ErrorKind::Other, err));
            }
        };
        println!(
            "subnet_o10r_canister_id {} canister_ids_len {:?}",
            subnet_o10r_canister_id,
            canister_ids.len()
        );

        canister_ids_list.extend(canister_ids);
    }

    // Add well known canisters
    canister_ids_list.extend(vec![
        Principal::from_text("74zq4-iqaaa-aaaam-ab53a-cai").unwrap(), // platform canister
        Principal::from_text("y6yjf-jyaaa-aaaal-qbd6q-cai").unwrap(), // post cache 1 canister
        Principal::from_text("zyajx-3yaaa-aaaag-acoga-cai").unwrap(), // post cache 2 canister
    ]);
    canister_ids_list.extend(subnet_o10r_ids);

    Ok(canister_ids_list)
}

pub async fn get_user_and_canisters_list(
    agent: &Agent,
) -> Result<HashMap<Principal, Vec<(Principal, Principal)>>, Error> {
    // Platform Orchestrator canister id
    // let pf_o10r_canister_id = Principal::from_text(PLATFORM_ORCHESTRATOR_ID).unwrap();

    // // Get the list of subnet orchestrator canister ids

    // let response = match agent
    //     .query(&pf_o10r_canister_id, "get_all_subnet_orchestrators")
    //     .with_arg(encode_args(()).unwrap())
    //     .call()
    //     .await
    // {
    //     Ok(response) => response,
    //     Err(err) => {
    //         println!("Unable to call the method, error: {:?}", err);
    //         return Err(Error::new(std::io::ErrorKind::Other, err));
    //     }
    // };

    // let subnet_o10r_ids = match candid::decode_one(&response) {
    //     Ok(result) => {
    //         let result: Vec<Principal> = result;
    //         result
    //     }
    //     Err(err) => {
    //         println!("Unable to decode the response, error: {:?}", err);
    //         return Err(Error::new(std::io::ErrorKind::Other, err));
    //     }
    // };
    // println!(
    //     "subnet_o10r_ids {:?}",
    //     subnet_o10r_ids
    //         .iter()
    //         .map(|x| x.to_string())
    //         .collect::<Vec<String>>()
    // );
    let subnet_o10r_ids = vec![Principal::from_text("br5f7-7uaaa-aaaaa-qaaca-cai").unwrap()];

    let mut user_canister_ids_map = HashMap::new();

    // Iterate over the subnet orchestrator canister ids
    for subnet_o10r_canister_id in subnet_o10r_ids {
        // Get individual canister list

        // skip if the subnet orchestrator is `rimrc-piaaa-aaaao-aaljq-cai`
        if subnet_o10r_canister_id == Principal::from_text("rimrc-piaaa-aaaao-aaljq-cai").unwrap() {
            continue;
        }

        let response = match agent
            .query(&subnet_o10r_canister_id, "get_user_id_and_canister_list")
            .with_arg(encode_args(()).unwrap())
            .call()
            .await
        {
            Ok(response) => response,
            Err(err) => {
                println!("Unable to call the method, error: {:?}", err);
                return Err(Error::new(std::io::ErrorKind::Other, err));
            }
        };

        let user_canister_ids = match candid::decode_one(&response) {
            Ok(result) => {
                let result: Vec<(Principal, Principal)> = result;
                result
            }
            Err(err) => {
                println!("Unable to decode the response, error: {:?}", err);
                return Err(Error::new(std::io::ErrorKind::Other, err));
            }
        };
        println!(
            "subnet_o10r_canister_id {} canister_ids_len {:?}",
            subnet_o10r_canister_id,
            user_canister_ids.len()
        );

        user_canister_ids_map.insert(subnet_o10r_canister_id, user_canister_ids);
    }

    Ok(user_canister_ids_map)
}
