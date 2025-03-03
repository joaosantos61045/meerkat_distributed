use tokio;

mod tests;

pub mod comm;
pub mod frontend;
pub mod runtime;

#[tokio::main]
pub async fn main() {
    runtime::repl::repl().await;
    // // comm::process_remote().await
    // todo!()
}