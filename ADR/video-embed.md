flowchart TD
    Cansiter1[(Canister 1)]
    Cansiter2[(Canister 2)]
    OffChainAgent[OffChain Agent]
    Frontend[Frontend SSR]
    AirflowJob[Airflow Job<br>1 hr<br>2.ML.GENERATE_EMBEDDING]
    DOT[Object Table]
    EmbedTable[Video Embed]
    YralVideoBucket[Yral Videos<br>Auto Storage]

    Frontend --[1]--> OffChainAgent

    OffChainAgent --> OnChain
    OffChainAgent ---> GCS

    DOT -.1:1.- YralVideoBucket

    AirflowJob --1.SELECT--> DOT
    AirflowJob --3.INSERT INTO--> EmbedTable

    subgraph OnChain
        Cansiter1
        Cansiter2
    end

    subgraph BigQuery[BigQuery]
        DOT
        EmbedTable
    end

    subgraph GCS[Google Cloud Storage]
        YralVideoBucket
    end
