# Off-Chain-Agent

## Overview

Off-chain agent is a service that runs on Fly.io and is responsible for orchestrating the off-chain operations of the platform. It is responsible for the following:

- Daily backups for disaster recovery
- Monitoring

## Architecture 

### Periodic Backups

```mermaid
flowchart TD
    IndividualCansiter1[(Individual User <br>Canister 1)]
    IndividualCansiter2[(Individual User <br>Canister 2)]
    UserIndex[(User Index)]
    PlatformOrchestrator[Platform <br>Orchestrator]
    OffChainAgent[OffChain Agent]
    CFWorker[Cloudflare Worker]
    R2[(R2 store)]

    OffChainAgent --[1.1]--> PlatformOrchestrator
    OffChainAgent --[1.2]--> UserIndex
    OffChainAgent --[2(get snapshot)]--> IndividualCansiter1
    OffChainAgent --[2.1(store snapshot)]--> R2
    OffChainAgent --[3]--> IndividualCansiter2
    OffChainAgent --[3.1]--> R2

    CFWorker -- 1(cron trigger) --> OffChainAgent


    subgraph OnChain
        PlatformOrchestrator
        UserIndex
        IndividualCansiter1
        IndividualCansiter2
    end
```

### Monitoring

```mermaid
flowchart TD
    IndividualCansiter1[(Individual User <br>Canister 1)]
    IndividualCansiter2[(Individual User <br>Canister 2)]
    UserIndex[(User Index)]
    PlatformOrchestrator[Platform <br>Orchestrator]
    OffChainAgent[OffChain Agent]
    Prom[Prometheus]
    Grafana[Grafana]

    OffChainAgent --[1.1]--> PlatformOrchestrator
    OffChainAgent --[1.2]--> UserIndex

    Prom -- 1(http_sd_config <br> periodically fetch canisters list) --> OffChainAgent
    Prom -- 2 (/metrics) --> IndividualCansiter1
    Prom -- 2 (/metrics) --> IndividualCansiter2

    subgraph OnChain
        PlatformOrchestrator
        UserIndex
        IndividualCansiter1
        IndividualCansiter2
    end

    subgraph GoogleCloud[DigitalOcean]
        Prom --> Grafana
    end
```
