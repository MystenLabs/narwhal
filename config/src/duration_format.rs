// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Allow us to serialize and deserialize Duration values in a more
//! human friendly format (e.x in json files). When serialized then
//! a string of the following format is written: [number]ms , for
//! example "20ms". When deserialized, then a Duration is created.
use serde::{Deserialize, Deserializer};
use std::time::Duration;

pub fn deserialize<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(deserializer)?;

    if let Some(milis) = s.strip_suffix("ms") {
        return milis
            .parse::<u64>()
            .map(Duration::from_millis)
            .map_err(|e| serde::de::Error::custom(e.to_string()));
    } else if let Some(seconds) = s.strip_suffix('s') {
        return seconds
            .parse::<u64>()
            .map(Duration::from_secs)
            .map_err(|e| serde::de::Error::custom(e.to_string()));
    }

    Err(serde::de::Error::custom(format!(
        "Wrong format detected: {s}. It should be number in miliseconds, e.x 10ms"
    )))
}

#[cfg(test)]
mod tests {
    use crate::duration_format;
    use serde::Deserialize;
    use std::time::Duration;

    #[derive(Deserialize)]
    struct MockProperties {
        #[serde(with = "duration_format")]
        property_1: Duration,
        #[serde(with = "duration_format")]
        property_2: Duration,
    }

    #[test]
    fn parse_miliseconds_and_seconds() {
        // GIVEN
        let input = r#"{
             "property_1": "1000ms",
             "property_2": "8s"
          }"#;

        // WHEN
        let result: MockProperties =
            serde_json::from_str(input).expect("Couldn't deserialize string");

        // THEN
        assert_eq!(result.property_1.as_millis(), 1_000);
        assert_eq!(result.property_2.as_secs(), 8);
    }

    #[test]
    fn parse_error() {
        // GIVEN
        let input = r#"{
             "property_1": "1000 ms",
             "property_2": "8seconds"
          }"#;

        // WHEN
        let result = serde_json::from_str::<MockProperties>(input);

        // THEN
        assert!(result.is_err());
    }
}
