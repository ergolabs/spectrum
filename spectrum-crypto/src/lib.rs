#![feature(async_fn_in_trait)]

pub mod digest;
mod hash;
pub mod pubkey;
pub mod signature;

/// Some statement which can be verified against public data `P`.
pub trait VerifiableAgainst<P> {
    fn verify(&self, public_data: &P) -> bool;
}

/// Proof that given statement `S` is verified.
#[derive(Ord, PartialOrd, Eq, PartialEq, Clone, Debug)]
pub struct Verified<S>(pub S);

/// Some statement which can be verified against public data `P`.
pub trait AsyncVerifiable<P>: Send + Sync + Sized {
    type Err: Send;
    async fn verify(self, public_data: &P) -> Result<Verified<Self>, Self::Err>;
}
