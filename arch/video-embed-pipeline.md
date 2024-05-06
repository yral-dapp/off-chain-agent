# With CLoud Composer

```mermaid

flowchart
    User[User]
    SSR[SSR]
    OffChainAgent[OffChain <br>Agent]
    Kafka[Kafka]
    BigQuery[BigQuery]
    CFStream[Cloudflare<br>Stream]
    R2[(R2)]
    VecDB[(Upstash Vec DB)]
    ExtractMedia[Media Extraction]
    GenVideoEmbed[GenVideoEmbed]
    GenAudioEmbed[GenAudioEmbed]
    GenMetadataEmbed[GenMetadataEmbed]
    MLServer[ML Server -<br>one/separate]


    User --> SSR
    SSR -- gRPC --> OffChainAgent
    SSR -- video --> CFStream
    OffChainAgent -- gRPC --> Kafka
    Kafka -- Dataflow --> BigQuery
    Kafka -- ConsumeFromTopicOperator --> ExtractMedia

    ExtractMedia --> CFStream
    ExtractMedia --> R2
    ExtractMedia --> GenVideoEmbed
    ExtractMedia --> GenAudioEmbed
    ExtractMedia --> GenMetadataEmbed

    GenVideoEmbed -.-> VecDB
    GenAudioEmbed -.-> VecDB
    GenMetadataEmbed -.-> VecDB

    GenVideoEmbed -.-> MLServer
    GenAudioEmbed -.-> MLServer
    GenMetadataEmbed -.-> MLServer


    subgraph CloudComposer[Cloud Composer]
        ExtractMedia
        GenVideoEmbed
        GenAudioEmbed
        GenMetadataEmbed
    end

    subgraph Fly[fly.io]
        OffChainAgent
        SSR
        MLServer
    end

    subgraph GCP[GCP]
        CloudComposer
        Kafka
        BigQuery
    end
```

# Without CLoud Composer

```mermaid

flowchart
    User[User]
    SSR[SSR]
    OffChainAgent[OffChain <br>Agent]
    Kafka[Kafka]
    BigQuery[BigQuery]
    CFStream[Cloudflare<br>Stream]
    R2[(R2)]
    VecDB[(Upstash Vec DB)]
    ExtractMedia[Media Extraction]
    MLServer[ML Server -<br>one/separate]


    User --> SSR
    SSR -- gRPC --> OffChainAgent
    SSR -- video --> CFStream
    OffChainAgent -- gRPC --> Kafka
    Kafka -- Dataflow --> BigQuery

    Kafka -.-> ExtractMedia
    ExtractMedia -.-> Kafka
    ExtractMedia -.-> CFStream
    ExtractMedia --> R2

    MLServer --> Kafka
    MLServer -.-> R2
    MLServer --> VecDB

    subgraph Fly[fly.io]
        OffChainAgent
        SSR
        MLServer
        ExtractMedia
    end

    subgraph GCP[GCP]
        Kafka
        BigQuery
    end

```

## Cloud Composer

**with**

- extensible (easy to add new steps in pipeline)
- switching to batch operations is easy

**without**

- simple to start with
- tightly coupled, not modular
- adding new steps will require spinning up separate services or containers.

To be looked into

1. Model hosting

- FlyGPU or plain
- single or separate

2. Model periodic training and CI
3. Attempt Retry for failed jobs - Ariflow retry / exponential backoff + jitter
