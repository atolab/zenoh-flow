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

use crate::registry::RegistryClient;
use crate::registry::RegistryFileClient;
use crate::{
    model::component::{OperatorRecord, SinkRecord, SourceRecord},
    runtime::runners::{
        operator::{OperatorDeclaration, OperatorRunner},
        sink::{SinkDeclaration, SinkRunner},
        source::{SourceDeclaration, SourceRunner},
    },
    types::{ZFError, ZFResult},
    utils::hlc::PeriodicHLC,
};
use async_std::sync::Arc;
use libloading::Library;
use rand::seq::SliceRandom;
use uhlc::HLC;
use url::Url;

pub static CORE_VERSION: &str = env!("CARGO_PKG_VERSION");
pub static RUSTC_VERSION: &str = env!("RUSTC_VERSION");

pub struct ComponentLoader {
    pub registry_client: Option<RegistryClient>,
    pub file_client: RegistryFileClient,
}

impl ComponentLoader {
    pub async fn from_zenoh_session(
        zn: Arc<zenoh::net::Session>,
        z: Arc<zenoh::Zenoh>,
    ) -> ZFResult<Self> {
        let servers = RegistryClient::find_servers(zn.clone()).await?;
        let registry_client = match servers.choose(&mut rand::thread_rng()) {
            Some(entry_point) => {
                log::debug!("Selected entrypoint runtime: {:?}", entry_point);
                let client = RegistryClient::new(zn, *entry_point);
                Some(client)
            }
            None => {
                // Err(ZFError::Disconnected)
                None
            }
        };

        let file_client = RegistryFileClient::from(z);

        Ok(Self {
            registry_client,
            file_client,
        })
    }

    pub async fn load_operator(
        &self,
        record: OperatorRecord,
        hlc: Arc<HLC>,
        path: String,
    ) -> ZFResult<OperatorRunner> {
        let uri = Url::parse(&path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

        match uri.scheme() {
            "file" => unsafe { load_lib_operator(record, hlc, make_file_path(uri)) },
            "zfregistry" => Err(ZFError::Unimplemented),
            _ => Err(ZFError::Unimplemented),
        }
    }

    pub async fn load_source(
        &self,
        record: SourceRecord,
        hlc: PeriodicHLC,
        path: String,
    ) -> ZFResult<SourceRunner> {
        let uri = Url::parse(&path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

        match uri.scheme() {
            "file" => unsafe { load_lib_source(record, hlc, make_file_path(uri)) },
            "zfregistry" => Err(ZFError::Unimplemented),
            _ => Err(ZFError::Unimplemented),
        }
    }

    pub async fn load_sink(&self, record: SinkRecord, path: String) -> ZFResult<SinkRunner> {
        let uri = Url::parse(&path).map_err(|err| ZFError::ParsingError(format!("{}", err)))?;

        match uri.scheme() {
            "file" => unsafe { load_lib_sink(record, make_file_path(uri)) },
            "zfregistry" => Err(ZFError::Unimplemented),
            _ => Err(ZFError::Unimplemented),
        }
    }
}

/// Load the library of the operator.
///
/// # Safety
///
/// This function dynamically loads an external library, things can go wrong:
/// - it will panic if the symbol `zfoperator_declaration` is not found,
/// - be sure to *trust* the code you are loading.
pub unsafe fn load_lib_operator(
    record: OperatorRecord,
    hlc: Arc<HLC>,
    path: String,
) -> ZFResult<OperatorRunner> {
    log::debug!("Operator Loading {}", path);

    let library = Library::new(path)?;
    let decl = library
        .get::<*mut OperatorDeclaration>(b"zfoperator_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let operator = (decl.register)()?;

    let runner = OperatorRunner::new(record, hlc, operator, Some(library));
    Ok(runner)
}

/// Load the library of a source.
///
/// # Safety
///
/// This function dynamically loads an external library, things can go wrong:
/// - it will panic if the symbol `zfsource_declaration` is not found,
/// - be sure to *trust* the code you are loading.
pub unsafe fn load_lib_source(
    record: SourceRecord,
    hlc: PeriodicHLC,
    path: String,
) -> ZFResult<SourceRunner> {
    log::debug!("Source Loading {}", path);
    let library = Library::new(path)?;
    let decl = library
        .get::<*mut SourceDeclaration>(b"zfsource_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let source = (decl.register)()?;

    let runner = SourceRunner::new(record, hlc, source, Some(library));
    Ok(runner)
}

/// Load the library of a sink.
///
/// # Safety
///
/// This function dynamically loads an external library, things can go wrong:
/// - it will panic if the symbol `zfsink_declaration` is not found,
/// - be sure to *trust* the code you are loading.
pub unsafe fn load_lib_sink(record: SinkRecord, path: String) -> ZFResult<SinkRunner> {
    log::debug!("Sink Loading {}", path);
    let library = Library::new(path)?;

    let decl = library
        .get::<*mut SinkDeclaration>(b"zfsink_declaration\0")?
        .read();

    // version checks to prevent accidental ABI incompatibilities
    if decl.rustc_version != RUSTC_VERSION || decl.core_version != CORE_VERSION {
        return Err(ZFError::VersionMismatch);
    }

    let sink = (decl.register)()?;

    let runner = SinkRunner::new(record, sink, Some(library));
    Ok(runner)
}

pub fn make_file_path(uri: Url) -> String {
    match uri.host_str() {
        Some(h) => format!("{}{}", h, uri.path()),
        None => uri.path().to_string(),
    }
}
