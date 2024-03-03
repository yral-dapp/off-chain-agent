use candid::encode_args;
use ic_agent::{export::Principal, Agent};
use std::io::Error;

pub async fn get_canisters_list(agent: &Agent) -> Result<Vec<Principal>, Error> {
    // Platform Orchestrator canister id
    let pf_o10r_canister_id = Principal::from_text("74zq4-iqaaa-aaaam-ab53a-cai").unwrap();

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
