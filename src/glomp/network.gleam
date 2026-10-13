//// This module contrains TLS settings as well as network related
//// error definitions. You use these through `glomp.tls` and the client's
//// `Disconnect` events.

import gleam/dynamic.{type Dynamic}

/// Which certificates the client accepts from the broker over TLS. A
/// certificate is how the broker proves it is the server you meant to reach.
pub type Trust {
  /// Certificates signed by the system's authorities. The host name is
  /// checked.
  SystemCertificates
  /// Certificates signed by the authorities in this PEM file. The host name
  /// is checked.
  CertificateFile(path: String)
  /// Any certificate. Use it for testing only. With this setting anyone on
  /// the network can pose as the broker and read the traffic.
  Unverified
}

/// An error from the network. You get one when connecting to the broker
/// fails, or when reading from or writing to the connection fails.
pub type NetworkError {
  /// The other side closed the connection.
  ConnectionClosed
  /// The host refused the connection.
  ConnectionRefused
  ConnectionReset
  ConnectionAborted
  /// Glomp's own time limit ran out, such as the connect timeout.
  TimedOut
  /// The operating system's time limit for the connection ran out.
  ConnectionTimedOut
  SocketNotConnected
  BrokenPipe
  /// DNS found no address for the host.
  HostNotFound
  HostDown
  HostUnreachable
  NetworkDown
  NetworkUnreachable
  AddressInUse
  AddressNotAvailable
  AddressFamilyNotSupported
  /// This process has too many files open.
  TooManyOpenFiles
  /// The whole system has too many files open.
  SystemFileTableFull
  NoBufferSpace
  OutOfMemory
  PermissionDenied
  OperationNotPermitted
  InvalidArgument
  /// The TLS handshake failed with this alert. `description` is Erlang's
  /// text for it.
  TlsAlert(alert: TlsAlert, description: String)
  /// Erlang's `ssl` application failed to start. `reason` is its error.
  TlsNotAvailable(reason: Dynamic)
  /// Erlang's `ssl` rejected the options. `details` is its error.
  InvalidTlsOptions(details: Dynamic)
  /// Reading the certificate file failed. `reason` is Erlang's error.
  CertificateFileUnreadable(path: String, reason: Dynamic)
  /// Loading the system's certificate authorities failed. `reason` is
  /// Erlang's error.
  NoSystemCertificates(reason: Dynamic)
  /// Any other error. `reason` is Erlang's error.
  UnknownNetworkError(reason: Dynamic)
}

/// A TLS alert, which is the reason one side gives when it ends a TLS
/// handshake. See RFC 8446 section 6.
pub type TlsAlert {
  CloseNotify
  UnexpectedMessage
  BadRecordMac
  RecordOverflow
  HandshakeFailure
  BadCertificate
  UnsupportedCertificate
  CertificateRevoked
  CertificateExpired
  CertificateUnknown
  IllegalParameter
  UnknownCa
  AccessDenied
  DecodeError
  DecryptError
  ProtocolVersion
  InsufficientSecurity
  InternalError
  InappropriateFallback
  UserCancelled
  NoRenegotiation
  MissingExtension
  UnsupportedExtension
  UnrecognisedName
  BadCertificateStatusResponse
  UnknownPskIdentity
  CertificateRequired
  NoApplicationProtocol
  /// Any other alert.
  UnknownAlert(name: String)
}
