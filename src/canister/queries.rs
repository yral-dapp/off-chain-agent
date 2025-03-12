pub const GET_LATEST_NON_NSFW_POSTS_QUERY: &str = "
SELECT
  t0.uri,
  (
  SELECT
    value
  FROM
    UNNEST(t0.metadata)
  WHERE
    name = 'timestamp') AS timestamp,
  (
  SELECT
    value
  FROM
    UNNEST(t0.metadata)
  WHERE
    name = 'canister_id') AS canister_id,
  (
  SELECT
    value
  FROM
    UNNEST(t0.metadata)
  WHERE
    name = 'post_id') AS post_id,
  video_nsfw_agg.probability
FROM
  `hot-or-not-feed-intelligence`.`yral_ds`.`video_embeddings` AS t0
INNER JOIN
  `hot-or-not-feed-intelligence`.`yral_ds`.`video_nsfw_agg` AS video_nsfw_agg
ON
  t0.uri = video_nsfw_agg.gcs_video_id
LEFT JOIN
  `hot-or-not-feed-intelligence`.`yral_ds`.`video_deleted` AS video_deleted
ON
  t0.uri = video_deleted.gcs_video_id
WHERE
  video_nsfw_agg.probability < 0.5
  AND video_deleted.gcs_video_id IS NULL
GROUP BY
  1,
  2,
  3,
  4,
  5
ORDER BY
  timestamp DESC
LIMIT
  50;
";

pub const GET_LATEST_NSFW_POSTS_QUERY: &str = "
SELECT
  t0.uri,
  (
  SELECT
    value
  FROM
    UNNEST(t0.metadata)
  WHERE
    name = 'timestamp') AS timestamp,
  (
  SELECT
    value
  FROM
    UNNEST(t0.metadata)
  WHERE
    name = 'canister_id') AS canister_id,
  (
  SELECT
    value
  FROM
    UNNEST(t0.metadata)
  WHERE
    name = 'post_id') AS post_id,
  video_nsfw_agg.probability
FROM
  `hot-or-not-feed-intelligence`.`yral_ds`.`video_embeddings` AS t0
INNER JOIN
  `hot-or-not-feed-intelligence`.`yral_ds`.`video_nsfw_agg` AS video_nsfw_agg
ON
  t0.uri = video_nsfw_agg.gcs_video_id
LEFT JOIN
  `hot-or-not-feed-intelligence`.`yral_ds`.`video_deleted` AS video_deleted
ON
  t0.uri = video_deleted.gcs_video_id
WHERE
  video_nsfw_agg.probability > 0.6
  AND video_deleted.gcs_video_id IS NULL
GROUP BY
  1,
  2,
  3,
  4,
  5
ORDER BY
  timestamp DESC
LIMIT
  50;
";
