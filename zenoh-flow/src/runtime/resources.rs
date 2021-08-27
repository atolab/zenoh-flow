//
// Copyright (c) 2017, 2021 ADLINK Technology Inc.
//
// This program and the accompanying materials are made available under the
// terms of the Eclipse Public License 2.0 which is available at
// http://www.eclipse.org/legal/epl-2.0, or the Apache License, Version 2.0
// which is available at https://www.apache.org/licenses/LICENSE-2.0.
//
// SPDX-License-Identifier: EPL-2.0 OR Apache-2.0
//
// Contributors:
//   ADLINK zenoh team, <zenoh@adlink-labs.tech>
//

#![allow(unused)]

#[cfg(feature = "data_bincode")]
extern crate bincode;

#[cfg(feature = "data_cbor")]
extern crate serde_cbor;

#[cfg(feature = "data_json")]
extern crate serde_json;

use crate::model::dataflow::DataFlowRecord;
use crate::runtime::{ZFRuntimeConfig, ZFRuntimeInfo, ZFRuntimeStatus};
use crate::serde::{de::DeserializeOwned, Serialize};
use crate::{async_std::sync::Arc, ZFError, ZFResult};
use futures::StreamExt;
use std::convert::TryFrom;
use uuid::Uuid;

pub static ROOT_PLUGIN_RUNTIME_PREFIX: &str = "/@/router/";
pub static ROOT_PLUGIN_RUNTIME_SUFFIX: &str = "/plugin/zenoh-flow";
pub static ROOT_STANDALONE: &str = "/zenoh-flow";

pub static KEY_RUNTIMES: &str = "runtimes";
pub static KEY_REGISTRY: &str = "registry";

pub static KEY_FLOWS: &str = "flows";
pub static KEY_GRAPHS: &str = "graphs";

pub static KEY_INFO: &str = "info";
pub static KEY_STATUS: &str = "status";
pub static KEY_CONFIGURATION: &str = "configuration";

#[macro_export]
macro_rules! RT_INFO_PATH {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_INFO
        )
    };
}

#[macro_export]
macro_rules! RT_STATUS_PATH {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_STATUS
        )
    };
}

#[macro_export]
macro_rules! RT_CONFIGURATION_PATH {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_CONFIGURATION
        )
    };
}

#[macro_export]
macro_rules! RT_FLOW_PATH {
    ($prefix:expr, $rtid:expr, $fid:expr, $iid:expr) => {
        format!(
            "{}/{}/{}/{}/{}/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS,
            $fid,
            $iid
        )
    };
}

#[macro_export]
macro_rules! RT_FLOW_SELECTOR_BY_INSTANCE {
    ($prefix:expr, $rtid:expr, $iid:expr) => {
        format!(
            "{}/{}/{}/{}/*/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS,
            $iid
        )
    };
}

#[macro_export]
macro_rules! RT_FLOW_SELECTOR_BY_FLOW {
    ($prefix:expr, $rtid:expr, $fid:expr) => {
        format!(
            "{}/{}/{}/{}/{}/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS,
            $fid
        )
    };
}

#[macro_export]
macro_rules! RT_FLOW_SELECTOR_ALL {
    ($prefix:expr, $rtid:expr) => {
        format!(
            "{}/{}/{}/{}/*/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $rtid,
            $crate::runtime::resources::KEY_FLOWS
        )
    };
}

#[macro_export]
macro_rules! FLOW_SELECTOR_BY_INSTANCE {
    ($prefix:expr, $iid:expr) => {
        format!(
            "{}/{}/*/{}/*/{}",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $crate::runtime::resources::KEY_FLOWS,
            $iid
        )
    };
}

#[macro_export]
macro_rules! FLOW_SELECTOR_BY_FLOW {
    ($prefix:expr, $fid:expr) => {
        format!(
            "{}/{}/*/{}/{}/*",
            $prefix,
            $crate::runtime::resources::KEY_RUNTIMES,
            $crate::runtime::resources::KEY_FLOWS,
            $fid
        )
    };
}

// Ser/De utils
pub fn deserialize_data<T>(raw_data: &[u8]) -> ZFResult<T>
where
    T: DeserializeOwned,
{
    #[cfg(feature = "data_bincode")]
    return Ok(bincode::deserialize::<T>(&raw_data)?);

    #[cfg(feature = "data_cbor")]
    return Ok(serde_cbor::from_slice::<T>(&raw_data)?);

    #[cfg(feature = "data_json")]
    return Ok(serde_json::from_str::<T>(std::str::from_utf8(raw_data)?)?);
}

#[cfg(feature = "data_bincode")]
pub fn serialize_data<T: ?Sized>(data: &T) -> FResult<Vec<u8>>
where
    T: Serialize,
{
    Ok(bincode::serialize(data)?)
}

#[cfg(feature = "data_json")]
pub fn serialize_data<T: ?Sized>(data: &T) -> ZFResult<Vec<u8>>
where
    T: Serialize,
{
    Ok(serde_json::to_string(data)?.into_bytes())
}

#[cfg(feature = "data_cbor")]
pub fn serialize_data<T>(data: &T) -> FResult<Vec<u8>>
where
    T: Serialize,
{
    Ok(serde_cbor::to_vec(data)?)
}
//

#[derive(Clone)]
pub struct ZFDataStore {
    //Name TBD
    z: Arc<zenoh::Zenoh>,
}

impl ZFDataStore {
    pub fn new(z: Arc<zenoh::Zenoh>) -> Self {
        Self { z }
    }

    pub async fn get_runtime_info(&self, rtid: Uuid) -> ZFResult<ZFRuntimeConfig> {
        let selector = zenoh::Selector::try_from(RT_INFO_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        let mut ds = ws.get(&selector).await?;

        // Not sure this is needed...
        let data = ds.collect::<Vec<zenoh::Data>>().await;

        match data.len() {
            0 => Err(ZFError::Empty),
            1 => {
                let kv = &data[0];
                match &kv.value {
                    zenoh::Value::Raw(_, buf) => {
                        let ni = deserialize_data::<ZFRuntimeConfig>(&buf.to_vec())?;
                        Ok(ni)
                    }
                    _ => Err(ZFError::DeseralizationError),
                }
            }
            _ => Err(ZFError::InvalidData(String::from(
                "Got more than one data for a single runtime information",
            ))),
        }
    }

    pub async fn remove_runtime_info(&self, rtid: Uuid) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_INFO_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        Ok(ws.delete(&path).await?)
    }

    pub async fn add_runtime_info(&self, rtid: Uuid, rt_info: ZFRuntimeConfig) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_INFO_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        let encoded_info = serialize_data(&rt_info)?;
        Ok(ws.put(&path, encoded_info.into()).await?)
    }

    pub async fn get_runtime_config(&self, rtid: Uuid) -> ZFResult<ZFRuntimeConfig> {
        let selector = zenoh::Selector::try_from(RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        let mut ds = ws.get(&selector).await?;

        // Not sure this is needed...
        let data = ds.collect::<Vec<zenoh::Data>>().await;

        match data.len() {
            0 => Err(ZFError::Empty),
            1 => {
                let kv = &data[0];
                match &kv.value {
                    zenoh::Value::Raw(_, buf) => {
                        let ni = deserialize_data::<ZFRuntimeConfig>(&buf.to_vec())?;
                        Ok(ni)
                    }
                    _ => Err(ZFError::DeseralizationError),
                }
            }
            _ => Err(ZFError::InvalidData(String::from(
                "Got more than one data for a single runtime information",
            ))),
        }
    }

    pub async fn remove_runtime_config(&self, rtid: Uuid) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        Ok(ws.delete(&path).await?)
    }

    pub async fn add_runtime_config(&self, rtid: Uuid, rt_info: ZFRuntimeConfig) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_CONFIGURATION_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        let encoded_info = serialize_data(&rt_info)?;
        Ok(ws.put(&path, encoded_info.into()).await?)
    }

    pub async fn get_runtime_status(&self, rtid: Uuid) -> ZFResult<ZFRuntimeStatus> {
        let selector = zenoh::Selector::try_from(RT_STATUS_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        let mut ds = ws.get(&selector).await?;

        // Not sure this is needed...
        let data = ds.collect::<Vec<zenoh::Data>>().await;

        match data.len() {
            0 => Err(ZFError::Empty),
            1 => {
                let kv = &data[0];
                match &kv.value {
                    zenoh::Value::Raw(_, buf) => {
                        let ni = deserialize_data::<ZFRuntimeStatus>(&buf.to_vec())?;
                        Ok(ni)
                    }
                    _ => Err(ZFError::DeseralizationError),
                }
            }
            _ => Err(ZFError::InvalidData(String::from(
                "Got more than one data for a single runtime information",
            ))),
        }
    }

    pub async fn remove_runtime_status(&self, rtid: Uuid) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_STATUS_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        Ok(ws.delete(&path).await?)
    }

    pub async fn add_runtime_status(&self, rtid: Uuid, rt_info: ZFRuntimeStatus) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_STATUS_PATH!(ROOT_STANDALONE, rtid))?;
        let ws = self.z.workspace(None).await?;
        let encoded_info = serialize_data(&rt_info)?;
        Ok(ws.put(&path, encoded_info.into()).await?)
    }

    pub async fn get_runtime_flow_by_instance(
        &self,
        rtid: Uuid,
        iid: Uuid,
    ) -> ZFResult<DataFlowRecord> {
        let selector =
            zenoh::Selector::try_from(RT_FLOW_SELECTOR_BY_INSTANCE!(ROOT_STANDALONE, rtid, iid))?;
        let ws = self.z.workspace(None).await?;
        let mut ds = ws.get(&selector).await?;

        // Not sure this is needed...
        let data = ds.collect::<Vec<zenoh::Data>>().await;

        match data.len() {
            0 => Err(ZFError::Empty),
            1 => {
                let kv = &data[0];
                match &kv.value {
                    zenoh::Value::Raw(_, buf) => {
                        let ni = deserialize_data::<DataFlowRecord>(&buf.to_vec())?;
                        Ok(ni)
                    }
                    _ => Err(ZFError::DeseralizationError),
                }
            }
            _ => Err(ZFError::InvalidData(String::from(
                "Got more than one data for a single runtime information",
            ))),
        }
    }

    pub async fn get_runtime_flow_instances(
        &self,
        rtid: Uuid,
        fid: String,
    ) -> ZFResult<Vec<DataFlowRecord>> {
        let selector =
            zenoh::Selector::try_from(RT_FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, rtid, fid))?;
        let ws = self.z.workspace(None).await?;
        let mut ds = ws.get(&selector).await?;

        // Not sure this is needed...
        let data = ds.collect::<Vec<zenoh::Data>>().await;
        let mut instances = Vec::new();

        for kv in data {
            match &kv.value {
                zenoh::Value::Raw(_, buf) => {
                    let ni = deserialize_data::<DataFlowRecord>(&buf.to_vec())?;
                    instances.push(ni);
                }
                _ => return Err(ZFError::DeseralizationError),
            }
        }

        Ok(instances)
    }

    pub async fn get_flow_instances(&self, fid: String) -> ZFResult<Vec<DataFlowRecord>> {
        let selector = zenoh::Selector::try_from(FLOW_SELECTOR_BY_FLOW!(ROOT_STANDALONE, fid))?;
        let ws = self.z.workspace(None).await?;
        let mut ds = ws.get(&selector).await?;

        // Not sure this is needed...
        let data = ds.collect::<Vec<zenoh::Data>>().await;
        let mut instances = Vec::new();

        for kv in data {
            match &kv.value {
                zenoh::Value::Raw(_, buf) => {
                    let ni = deserialize_data::<DataFlowRecord>(&buf.to_vec())?;
                    instances.push(ni);
                }
                _ => return Err(ZFError::DeseralizationError),
            }
        }

        Ok(instances)
    }

    pub async fn remove_runtime_flow_instance(
        &self,
        rtid: Uuid,
        fid: String,
        iid: Uuid,
    ) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_FLOW_PATH!(ROOT_STANDALONE, rtid, fid, iid))?;
        let ws = self.z.workspace(None).await?;
        Ok(ws.delete(&path).await?)
    }

    pub async fn add_runtime_flow(
        &self,
        rtid: Uuid,
        flow_instance: DataFlowRecord,
    ) -> ZFResult<()> {
        let path = zenoh::Path::try_from(RT_FLOW_PATH!(
            ROOT_STANDALONE,
            rtid,
            &flow_instance.flow,
            &flow_instance.uuid
        ))?;
        let ws = self.z.workspace(None).await?;
        let encoded_info = serialize_data(&flow_instance)?;
        Ok(ws.put(&path, encoded_info.into()).await?)
    }
}
