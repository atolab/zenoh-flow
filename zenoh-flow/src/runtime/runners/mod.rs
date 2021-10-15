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

pub mod connector;
pub mod operator;
pub mod sink;
pub mod source;

use crate::runtime::graph::link::{LinkReceiver, LinkSender};
use crate::runtime::message::Message;
use crate::runtime::runners::connector::{ZenohReceiver, ZenohReplay, ZenohSender};
use crate::runtime::runners::operator::OperatorRunner;
use crate::runtime::runners::sink::SinkRunner;
use crate::runtime::runners::source::SourceRunner;
use crate::types::ZFResult;
use crate::{OperatorId, PortId, ZFError};

use crate::async_std::prelude::*;
use crate::async_std::{
    channel::{bounded, Receiver, Sender},
    task::JoinHandle,
};

use futures_lite::future::FutureExt;
use std::pin::Pin;
use std::task::{Context, Poll};

#[derive(Clone, Debug)]
pub enum RunnerKind {
    Source,
    Operator,
    Sink,
    Connector,
    Replay,
}

pub struct RunnerManager {
    stopper: Sender<()>,
    handler: JoinHandle<ZFResult<()>>,
    kind: RunnerKind,
}

impl RunnerManager {
    pub fn new(stopper: Sender<()>, handler: JoinHandle<ZFResult<()>>, kind: RunnerKind) -> Self {
        Self {
            stopper,
            handler,
            kind,
        }
    }

    pub async fn kill(&self) -> ZFResult<()> {
        Ok(self.stopper.send(()).await?)
    }

    pub fn get_handler(&self) -> &JoinHandle<ZFResult<()>> {
        &self.handler
    }

    pub fn get_kind(&self) -> &RunnerKind {
        &self.kind
    }
}

impl Future for RunnerManager {
    type Output = ZFResult<()>;

    fn poll(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        self.handler.poll(ctx)
    }
}

pub enum RunAction {
    RestartRun(Option<ZFError>),
    Stop,
    StopError(ZFError),
}

#[derive(Clone)]
pub enum Runner {
    Operator(OperatorRunner),
    Source(SourceRunner),
    Sink(SinkRunner),
    Sender(ZenohSender),
    Receiver(ZenohReceiver),
    Replay(ZenohReplay),
}

impl std::fmt::Debug for Runner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Runner")
            .field("kind", &self.get_kind())
            .finish()
    }
}

impl Runner {
    pub async fn run(&self) -> ZFResult<RunAction> {
        match self {
            Runner::Operator(runner) => runner.run().await,
            Runner::Source(runner) => runner.run().await,
            Runner::Sink(runner) => runner.run().await,
            Runner::Sender(runner) => runner.run().await,
            Runner::Receiver(runner) => runner.run().await,
            Runner::Replay(runner) => runner.run().await,
        }
    }

    pub async fn add_input(
        &self,
        input: LinkReceiver<Message>,
        from_id: OperatorId,
    ) -> ZFResult<()> {
        log::trace!("add_input({:?})", input);
        match self {
            Runner::Operator(runner) => {
                runner.add_input(input, from_id).await;
                Ok(())
            }
            Runner::Source(_) => Err(ZFError::SourceDoNotHaveInputs),
            Runner::Sink(runner) => {
                runner.add_input(input, from_id).await;
                Ok(())
            }
            Runner::Sender(runner) => {
                runner.add_input(input, from_id).await;
                Ok(())
            }
            Runner::Receiver(_) => Err(ZFError::ReceiverDoNotHaveInputs),
            Runner::Replay(_) => Err(ZFError::ReceiverDoNotHaveInputs),
        }
    }

    pub async fn add_output(&self, output: LinkSender<Message>, to_id: OperatorId) -> ZFResult<()> {
        log::trace!("add_output({:?})", output);
        match self {
            Runner::Operator(runner) => {
                runner.add_output(output, to_id).await;
                Ok(())
            }
            Runner::Source(runner) => {
                runner.add_output(output, to_id).await;
                Ok(())
            }
            Runner::Sink(_) => Err(ZFError::SinkDoNotHaveOutputs),
            Runner::Sender(_) => Err(ZFError::SenderDoNotHaveOutputs),
            Runner::Receiver(runner) => {
                runner.add_output(output, to_id).await;
                Ok(())
            }
            Runner::Replay(runner) => {
                runner.add_output(output, to_id).await;
                Ok(())
            }
        }
    }

    pub async fn remove_output(&self, port_id: PortId, to_id: OperatorId) -> ZFResult<()> {
        log::trace!("remove_output({:?},{:?})", port_id, to_id);
        match self {
            Runner::Operator(runner) => {
                runner.remove_output(port_id, to_id).await;
                Ok(())
            }
            Runner::Source(runner) => {
                runner.remove_output(port_id, to_id).await;
                Ok(())
            }
            Runner::Sink(_) => Err(ZFError::SinkDoNotHaveOutputs),
            Runner::Sender(_) => Err(ZFError::SenderDoNotHaveOutputs),
            Runner::Receiver(runner) => {
                runner.remove_output(to_id).await;
                Ok(())
            }
            Runner::Replay(runner) => {
                runner.remove_output(to_id).await;
                Ok(())
            }
        }
    }

    pub async fn remove_input(&self, port_id: PortId, from_id: OperatorId) -> ZFResult<()> {
        log::trace!("remove_input({:?},{:?})", port_id, from_id);
        match self {
            Runner::Operator(runner) => {
                runner.remove_input(port_id, from_id).await;
                Ok(())
            }
            Runner::Source(_) => Err(ZFError::SourceDoNotHaveInputs),
            Runner::Sink(runner) => {
                runner.remove_input(port_id, from_id).await;
                Ok(())
            }
            Runner::Sender(runner) => {
                runner.remove_input(from_id).await;
                Ok(())
            }
            Runner::Receiver(_) => Err(ZFError::ReceiverDoNotHaveInputs),
            Runner::Replay(_) => Err(ZFError::ReceiverDoNotHaveInputs),
        }
    }

    pub async fn clean(&self) -> ZFResult<()> {
        match self {
            Runner::Operator(runner) => runner.clean().await,
            Runner::Source(runner) => runner.clean().await,
            Runner::Sink(runner) => runner.clean().await,
            Runner::Sender(_) => Err(ZFError::Unimplemented),
            Runner::Receiver(_) => Err(ZFError::Unimplemented),
            Runner::Replay(_) => Err(ZFError::Unimplemented),
        }
    }

    pub fn start(&self) -> RunnerManager {
        let (s, r) = bounded::<()>(1);
        let cloned_self = self.clone();

        let h = async_std::task::spawn(async move { cloned_self.run_stoppable(r).await });
        RunnerManager::new(s, h, self.get_kind())
    }

    pub async fn run_stoppable(&self, stop: Receiver<()>) -> ZFResult<()> {
        loop {
            let run = async {
                match self.run().await {
                    Ok(run_action) => run_action,
                    Err(e) => RunAction::RestartRun(Some(e)),
                }
            };
            let stopper = async {
                match stop.recv().await {
                    Ok(_) => RunAction::Stop,
                    Err(e) => RunAction::StopError(e.into()),
                }
            };

            match run.race(stopper).await {
                RunAction::RestartRun(e) => {
                    log::error!("The run loop exited with {:?}, restarting...", e);
                    continue;
                }
                RunAction::Stop => {
                    log::trace!("Received kill command, killing runner");
                    break Ok(());
                }
                RunAction::StopError(e) => {
                    log::error!("The stopper recv got an error: {}, exiting...", e);
                    break Err(e);
                }
            }
        }
    }

    pub fn get_kind(&self) -> RunnerKind {
        match self {
            Runner::Operator(_) => RunnerKind::Operator,
            Runner::Source(_) => RunnerKind::Source,
            Runner::Sink(_) => RunnerKind::Sink,
            Runner::Sender(_) => RunnerKind::Connector,
            Runner::Receiver(_) => RunnerKind::Connector,
            Runner::Replay(_) => RunnerKind::Replay,
        }
    }
}

#[macro_export]
macro_rules! run_input_rules {
    ($node: expr, $tokens : expr, $links : expr, $state: expr, $context: expr) => {
        while !$links.is_empty() {
            match future::select_all($links).await {
                // this could be "slow" as suggested by LC
                (Ok((id, message)), _i, remaining) => {
                    match message.as_ref() {
                        Message::Data(_) => {
                            $tokens.insert(id, Token::from(message));

                            match $node.input_rule($context, $state, &mut $tokens) {
                                Ok(true) => {
                                    // we can run
                                    log::debug!("IR: OK");
                                    $links = vec![]; // this makes the while loop to end
                                }
                                Ok(false) => {
                                    //we cannot run, we should update the list of futures
                                    log::debug!("IR: Not OK");
                                    $links = remaining;
                                }
                                Err(_) => {
                                    // we got an error on the input rules, we should recover/update list of futures
                                    log::debug!("IR: received an error");
                                    $links = remaining;
                                }
                            }
                        }
                        Message::Control(_) => {
                            //control message receiver, we should handle it
                            $links = remaining;
                        }
                    };
                }
                (Err(e), i, remaining) => {
                    log::debug!("Link index {:?} has got error {:?}", i, e);
                    $links = remaining;
                }
            }
        };
        drop($links);
    };
}
