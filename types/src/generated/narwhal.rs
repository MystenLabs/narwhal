// Copyright (c) 2022, Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0
// TODO: Make proto the source of truth for all relevant narwhal types.

/// 
/// Proto wrapper for Narwhal type 
///
/// pub struct CertificateDigest([u8; DIGEST_LEN])
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CertificateDigest {
    #[prost(bytes="vec", tag="1")]
    pub f_bytes: ::prost::alloc::vec::Vec<u8>,
}
/// 
/// Proto wrapper for Narwhal type 
///
/// pub struct BatchDigest(pub [u8; crypto::DIGEST_LEN]);
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchDigest {
    #[prost(bytes="vec", tag="1")]
    pub f_bytes: ::prost::alloc::vec::Vec<u8>,
}
/// 
/// Proto wrapper for Narwhal type 
///
/// pub struct Batch(pub Vec<Transaction>);
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Batch {
    #[prost(message, repeated, tag="1")]
    pub transaction: ::prost::alloc::vec::Vec<Transaction>,
}
/// 
/// Proto wrapper for Narwhal type 
///
/// pub type Transaction = Vec<u8>;
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct Transaction {
    #[prost(bytes="vec", tag="1")]
    pub f_bytes: ::prost::alloc::vec::Vec<u8>,
}
///
/// Proto wrapper for Narwhal type 
///
/// pub struct BlockError {
///     id: CertificateDigest,
///     error: BlockErrorType,
/// }
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionError {
    #[prost(message, optional, tag="1")]
    pub id: ::core::option::Option<CertificateDigest>,
    #[prost(enumeration="CollectionErrorType", tag="2")]
    pub error: i32,
}
///
/// Proto wrapper for Narwhal type
///
/// pub struct BatchMessage {
///   pub id: BatchDigest,
///   pub transactions: Batch,
/// }
///
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct BatchMessage {
    #[prost(message, optional, tag="1")]
    pub id: ::core::option::Option<BatchDigest>,
    #[prost(message, optional, tag="2")]
    pub transactions: ::core::option::Option<Batch>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct CollectionRetrievalResult {
    #[prost(oneof="collection_retrieval_result::RetrievalResult", tags="1, 2")]
    pub retrieval_result: ::core::option::Option<collection_retrieval_result::RetrievalResult>,
}
/// Nested message and enum types in `CollectionRetrievalResult`.
pub mod collection_retrieval_result {
    #[derive(Clone, PartialEq, ::prost::Oneof)]
    pub enum RetrievalResult {
        #[prost(message, tag="1")]
        Batch(super::BatchMessage),
        #[prost(message, tag="2")]
        Error(super::CollectionError),
    }
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionsRequest {
    /// List of collections to be retreived.
    #[prost(message, repeated, tag="1")]
    pub collection_ids: ::prost::alloc::vec::Vec<CertificateDigest>,
}
#[derive(Clone, PartialEq, ::prost::Message)]
pub struct GetCollectionsResponse {
    /// List of retrieval results of collections.
    #[prost(message, repeated, tag="1")]
    pub result: ::prost::alloc::vec::Vec<CollectionRetrievalResult>,
}
///
/// Proto wrapper for Narwhal type 
///
/// pub enum BlockErrorType {
///   BlockNotFound,
///   BatchTimeout,
///   BatchError,
/// }
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
#[repr(i32)]
pub enum CollectionErrorType {
    CollectionNotFound = 0,
    CollectionTimeout = 1,
    CollectionError = 2,
}
/// Generated client implementations.
pub mod validator_client {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    /// The consensus to mempool interface for validator actions.
    #[derive(Debug, Clone)]
    pub struct ValidatorClient<T> {
        inner: tonic::client::Grpc<T>,
    }
    impl ValidatorClient<tonic::transport::Channel> {
        /// Attempt to create a new client by connecting to a given endpoint.
        pub async fn connect<D>(dst: D) -> Result<Self, tonic::transport::Error>
        where
            D: std::convert::TryInto<tonic::transport::Endpoint>,
            D::Error: Into<StdError>,
        {
            let conn = tonic::transport::Endpoint::new(dst)?.connect().await?;
            Ok(Self::new(conn))
        }
    }
    impl<T> ValidatorClient<T>
    where
        T: tonic::client::GrpcService<tonic::body::BoxBody>,
        T::Error: Into<StdError>,
        T::ResponseBody: Body<Data = Bytes> + Send + 'static,
        <T::ResponseBody as Body>::Error: Into<StdError> + Send,
    {
        pub fn new(inner: T) -> Self {
            let inner = tonic::client::Grpc::new(inner);
            Self { inner }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> ValidatorClient<InterceptedService<T, F>>
        where
            F: tonic::service::Interceptor,
            T::ResponseBody: Default,
            T: tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
                Response = http::Response<
                    <T as tonic::client::GrpcService<tonic::body::BoxBody>>::ResponseBody,
                >,
            >,
            <T as tonic::codegen::Service<
                http::Request<tonic::body::BoxBody>,
            >>::Error: Into<StdError> + Send + Sync,
        {
            ValidatorClient::new(InterceptedService::new(inner, interceptor))
        }
        /// Compress requests with `gzip`.
        ///
        /// This requires the server to support it otherwise it might respond with an
        /// error.
        #[must_use]
        pub fn send_gzip(mut self) -> Self {
            self.inner = self.inner.send_gzip();
            self
        }
        /// Enable decompressing responses with `gzip`.
        #[must_use]
        pub fn accept_gzip(mut self) -> Self {
            self.inner = self.inner.accept_gzip();
            self
        }
        /// Returns the collection contents for each requested collection
        pub async fn get_collections(
            &mut self,
            request: impl tonic::IntoRequest<super::GetCollectionsRequest>,
        ) -> Result<tonic::Response<super::GetCollectionsResponse>, tonic::Status> {
            self.inner
                .ready()
                .await
                .map_err(|e| {
                    tonic::Status::new(
                        tonic::Code::Unknown,
                        format!("Service was not ready: {}", e.into()),
                    )
                })?;
            let codec = tonic::codec::ProstCodec::default();
            let path = http::uri::PathAndQuery::from_static(
                "/narwhal.Validator/GetCollections",
            );
            self.inner.unary(request.into_request(), path, codec).await
        }
    }
}
/// Generated server implementations.
pub mod validator_server {
    #![allow(unused_variables, dead_code, missing_docs, clippy::let_unit_value)]
    use tonic::codegen::*;
    ///Generated trait containing gRPC methods that should be implemented for use with ValidatorServer.
    #[async_trait]
    pub trait Validator: Send + Sync + 'static {
        /// Returns the collection contents for each requested collection
        async fn get_collections(
            &self,
            request: tonic::Request<super::GetCollectionsRequest>,
        ) -> Result<tonic::Response<super::GetCollectionsResponse>, tonic::Status>;
    }
    /// The consensus to mempool interface for validator actions.
    #[derive(Debug)]
    pub struct ValidatorServer<T: Validator> {
        inner: _Inner<T>,
        accept_compression_encodings: (),
        send_compression_encodings: (),
    }
    struct _Inner<T>(Arc<T>);
    impl<T: Validator> ValidatorServer<T> {
        pub fn new(inner: T) -> Self {
            Self::from_arc(Arc::new(inner))
        }
        pub fn from_arc(inner: Arc<T>) -> Self {
            let inner = _Inner(inner);
            Self {
                inner,
                accept_compression_encodings: Default::default(),
                send_compression_encodings: Default::default(),
            }
        }
        pub fn with_interceptor<F>(
            inner: T,
            interceptor: F,
        ) -> InterceptedService<Self, F>
        where
            F: tonic::service::Interceptor,
        {
            InterceptedService::new(Self::new(inner), interceptor)
        }
    }
    impl<T, B> tonic::codegen::Service<http::Request<B>> for ValidatorServer<T>
    where
        T: Validator,
        B: Body + Send + 'static,
        B::Error: Into<StdError> + Send + 'static,
    {
        type Response = http::Response<tonic::body::BoxBody>;
        type Error = std::convert::Infallible;
        type Future = BoxFuture<Self::Response, Self::Error>;
        fn poll_ready(
            &mut self,
            _cx: &mut Context<'_>,
        ) -> Poll<Result<(), Self::Error>> {
            Poll::Ready(Ok(()))
        }
        fn call(&mut self, req: http::Request<B>) -> Self::Future {
            let inner = self.inner.clone();
            match req.uri().path() {
                "/narwhal.Validator/GetCollections" => {
                    #[allow(non_camel_case_types)]
                    struct GetCollectionsSvc<T: Validator>(pub Arc<T>);
                    impl<
                        T: Validator,
                    > tonic::server::UnaryService<super::GetCollectionsRequest>
                    for GetCollectionsSvc<T> {
                        type Response = super::GetCollectionsResponse;
                        type Future = BoxFuture<
                            tonic::Response<Self::Response>,
                            tonic::Status,
                        >;
                        fn call(
                            &mut self,
                            request: tonic::Request<super::GetCollectionsRequest>,
                        ) -> Self::Future {
                            let inner = self.0.clone();
                            let fut = async move {
                                (*inner).get_collections(request).await
                            };
                            Box::pin(fut)
                        }
                    }
                    let accept_compression_encodings = self.accept_compression_encodings;
                    let send_compression_encodings = self.send_compression_encodings;
                    let inner = self.inner.clone();
                    let fut = async move {
                        let inner = inner.0;
                        let method = GetCollectionsSvc(inner);
                        let codec = tonic::codec::ProstCodec::default();
                        let mut grpc = tonic::server::Grpc::new(codec)
                            .apply_compression_config(
                                accept_compression_encodings,
                                send_compression_encodings,
                            );
                        let res = grpc.unary(method, req).await;
                        Ok(res)
                    };
                    Box::pin(fut)
                }
                _ => {
                    Box::pin(async move {
                        Ok(
                            http::Response::builder()
                                .status(200)
                                .header("grpc-status", "12")
                                .header("content-type", "application/grpc")
                                .body(empty_body())
                                .unwrap(),
                        )
                    })
                }
            }
        }
    }
    impl<T: Validator> Clone for ValidatorServer<T> {
        fn clone(&self) -> Self {
            let inner = self.inner.clone();
            Self {
                inner,
                accept_compression_encodings: self.accept_compression_encodings,
                send_compression_encodings: self.send_compression_encodings,
            }
        }
    }
    impl<T: Validator> Clone for _Inner<T> {
        fn clone(&self) -> Self {
            Self(self.0.clone())
        }
    }
    impl<T: std::fmt::Debug> std::fmt::Debug for _Inner<T> {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{:?}", self.0)
        }
    }
    impl<T: Validator> tonic::transport::NamedService for ValidatorServer<T> {
        const NAME: &'static str = "narwhal.Validator";
    }
}
