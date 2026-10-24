use std::env;
use std::process::{Command, exit};
use std::time::Duration;
use tokio::time::{sleep, Instant};

#[derive(Debug)]
struct Config {
    endpoint: String,
    api_key: String,
    model: String,
    task_id: String,
    session_id: String,
    base_interval: Duration,
    max_interval: Duration,
    max_wait: Duration,
    on_success_cmd: Option<String>,
}

fn parse_args() -> Result<Config, String> {
    let args: Vec<String> = env::args().collect();
    let mut endpoint = None;
    let mut provider = None;
    let mut api_key = None;
    let mut api_key_env = None;
    let mut model = "glm-5.3-flash".to_string();
    let mut task_id = String::new();
    let mut session_id = String::new();
    let mut base_interval_secs = 2.0f64;
    let mut max_interval_secs = 900.0f64; // 15 mins
    let mut max_wait_secs = 21600.0f64; // 6 hours
    let mut on_success_cmd = None;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--endpoint" => {
                i += 1;
                if i < args.len() {
                    endpoint = Some(args[i].clone());
                }
            }
            "--provider" => {
                i += 1;
                if i < args.len() {
                    provider = Some(args[i].clone());
                }
            }
            "--api-key" => {
                i += 1;
                if i < args.len() {
                    api_key = Some(args[i].clone());
                }
            }
            "--api-key-env" => {
                i += 1;
                if i < args.len() {
                    api_key_env = Some(args[i].clone());
                }
            }
            "--model" => {
                i += 1;
                if i < args.len() {
                    model = args[i].clone();
                }
            }
            "--task-id" => {
                i += 1;
                if i < args.len() {
                    task_id = args[i].clone();
                }
            }
            "--session-id" => {
                i += 1;
                if i < args.len() {
                    session_id = args[i].clone();
                }
            }
            "--base-interval-secs" => {
                i += 1;
                if i < args.len() {
                    base_interval_secs = args[i].parse().unwrap_or(2.0);
                }
            }
            "--max-interval-secs" => {
                i += 1;
                if i < args.len() {
                    max_interval_secs = args[i].parse().unwrap_or(900.0);
                }
            }
            "--max-wait-secs" => {
                i += 1;
                if i < args.len() {
                    max_wait_secs = args[i].parse().unwrap_or(21600.0);
                }
            }
            "--on-success-cmd" => {
                i += 1;
                if i < args.len() {
                    on_success_cmd = Some(args[i].clone());
                }
            }
            "-h" | "--help" => {
                println!(
                    "Usage: swarm_sentinel [--endpoint <URL> | --provider zai] [--model <MODEL>] [--api-key-env <VAR>] [--task-id <ID>] [--max-interval-secs 900] [--max-wait-secs 21600] [--on-success-cmd <CMD>]"
                );
                exit(0);
            }
            other => {
                return Err(format!("Unknown argument: {}", other));
            }
        }
        i += 1;
    }

    let endpoint = match (endpoint, provider.as_deref()) {
        (Some(e), _) => e,
        (None, Some("zai")) => "https://open.bigmodel.cn/api/paas/v4/chat/completions".to_string(),
        (None, Some("openai")) => "https://api.openai.com/v1/chat/completions".to_string(),
        (None, Some("openrouter")) => "https://openrouter.ai/api/v1/chat/completions".to_string(),
        (None, Some(other)) => {
            return Err(format!("Unknown provider '{}'. Pass explicit --endpoint <URL>", other));
        }
        (None, None) => "https://open.bigmodel.cn/api/paas/v4/chat/completions".to_string(),
    };

    let resolved_key = api_key
        .or_else(|| api_key_env.and_then(|var| env::var(var).ok()))
        .or_else(|| env::var("ZHIPU_API_KEY").ok())
        .or_else(|| env::var("OPENAI_API_KEY").ok())
        .or_else(|| env::var("ANTHROPIC_API_KEY").ok())
        .or_else(|| env::var("OPENROUTER_API_KEY").ok())
        .unwrap_or_default();

    Ok(Config {
        endpoint,
        api_key: resolved_key,
        model,
        task_id,
        session_id,
        base_interval: Duration::from_secs_f64(base_interval_secs),
        max_interval: Duration::from_secs_f64(max_interval_secs),
        max_wait: Duration::from_secs_f64(max_wait_secs),
        on_success_cmd,
    })
}

struct ProbeResult {
    status_code: u16,
    retry_after: Option<Duration>,
    error_message: String,
}

async fn probe(client: &reqwest::Client, config: &Config) -> ProbeResult {
    let payload = serde_json::json!({
        "model": config.model,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 1
    });

    let mut req = client
        .post(&config.endpoint)
        .header("Content-Type", "application/json")
        .header("User-Agent", "Jcode-Swarm-Sentinel/1.0");

    if !config.api_key.is_empty() {
        req = req.header("Authorization", format!("Bearer {}", config.api_key));
    }

    match req.json(&payload).send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            if status == 200 {
                return ProbeResult {
                    status_code: 200,
                    retry_after: None,
                    error_message: String::new(),
                };
            }

            let mut retry_after = None;
            if let Some(val) = resp.headers().get("Retry-After") {
                if let Ok(s) = val.to_str() {
                    if let Ok(secs) = s.trim().parse::<u64>() {
                        retry_after = Some(Duration::from_secs(secs));
                    }
                }
            }

            let body_str = resp.text().await.unwrap_or_default();
            if retry_after.is_none() {
                if let Ok(v) = serde_json::from_str::<serde_json::Value>(&body_str) {
                    if let Some(secs) = v.get("retry_after_seconds").and_then(|s| s.as_f64()) {
                        retry_after = Some(Duration::from_secs_f64(secs));
                    } else if let Some(secs) = v
                        .get("error")
                        .and_then(|e| e.get("retry_after_seconds"))
                        .and_then(|s| s.as_f64())
                    {
                        retry_after = Some(Duration::from_secs_f64(secs));
                    }
                }
            }

            ProbeResult {
                status_code: status,
                retry_after,
                error_message: body_str,
            }
        }
        Err(e) => ProbeResult {
            status_code: 0,
            retry_after: None,
            error_message: e.to_string(),
        },
    }
}

#[tokio::main]
async fn main() {
    let config = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("[SENTINEL ERROR] {}", e);
            exit(1);
        }
    };

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(15))
        .build()
        .expect("Failed to initialize reqwest client");

    let start = Instant::now();
    let mut attempt = 1;
    let mut current_interval = config.base_interval;

    println!(
        "[SENTINEL] Native Rust probe active for task='{}' session='{}'",
        config.task_id, config.session_id
    );
    println!(
        "[SENTINEL] Target: {} (model: {})",
        config.endpoint, config.model
    );
    println!(
        "[SENTINEL] Timeout: {:.1}h, Max interval: {:.1}m",
        config.max_wait.as_secs_f64() / 3600.0,
        config.max_interval.as_secs_f64() / 60.0
    );

    loop {
        let elapsed = start.elapsed();
        if elapsed >= config.max_wait {
            eprintln!(
                "[SENTINEL] TIMEOUT: Exceeded {:.1}h waiting for rate limit reset.",
                config.max_wait.as_secs_f64() / 3600.0
            );
            exit(1);
        }

        print!(
            "[SENTINEL] Probe #{} (elapsed: {:.1}s)... ",
            attempt,
            elapsed.as_secs_f64()
        );
        let res = probe(&client, &config).await;

        if res.status_code == 200 {
            println!(
                "SUCCESS (HTTP 200)! Rate limit cleared after {:.1}s ({:.1}m)!",
                elapsed.as_secs_f64(),
                elapsed.as_secs_f64() / 60.0
            );

            if let Some(cmd) = &config.on_success_cmd {
                println!("[SENTINEL] Executing success callback: {}", cmd);
                let _ = Command::new("sh").arg("-c").arg(cmd).status();
            }

            exit(0);
        }

        println!("Status: HTTP {} ({})", res.status_code, res.error_message);

        let sleep_duration = if let Some(ra) = res.retry_after {
            let clamped = ra.min(config.max_interval);
            println!(
                "[SENTINEL] Server Retry-After: {:.1}s (sleeping {:.1}s)",
                ra.as_secs_f64(),
                clamped.as_secs_f64()
            );
            clamped
        } else {
            let clamped = current_interval.min(config.max_interval);
            println!("[SENTINEL] Backoff delay: {:.1}s", clamped.as_secs_f64());
            current_interval = (current_interval * 2).min(config.max_interval);
            clamped
        };

        attempt += 1;
        sleep(sleep_duration).await;
    }
}
