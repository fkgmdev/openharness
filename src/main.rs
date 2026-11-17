use eventsource_stream::Eventsource;
use futures::StreamExt;
use reqwest::Client;
use serde_json::{Value, json};
use std::io::{Write, stdin, stdout};

#[derive(Default)]
struct ToolCall {
    id: String,
    name: String,
    args: String,
}

fn run_tool(name: &str, args: &str) -> String {
    let a: Value = serde_json::from_str(args).unwrap_or(Value::Null);
    let out = match name {
        "read_file" => {
            let p = a["path"].as_str().unwrap_or("");
            std::fs::read_to_string(p).unwrap_or_else(|e| format!("error: {e}"))
        }
        "write_file" => {
            let p = a["path"].as_str().unwrap_or("");
            let content = a["content"].as_str().unwrap_or("");

            match std::fs::write(p, content) {
                Ok(_) => format!("wrote {} bytes to {}", content.len(), p),
                Err(e) => format!("error: {e}"),
            }
        }
        _ => format!("unknown tool: {name}"),
    };
    out.chars().take(8000).collect()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let key = std::env::var("API_KEY")?;
    let mut messages: Vec<Value> = vec![];
    let client = Client::new();

    let tools = json!([
        {
            "type": "function",
            "function": {
                "name": "read_file",
                "description": "Read a file and return its contents",
                "parameters": {
                    "type": "object",
                    "properties": { "path": { "type": "string" } },
                    "required": ["path"]
                }
            }
        },
        {
            "type": "function",
            "function": {
                "name": "write_file",
                "description": "Write content to a file, overwriting it if it exists",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["path", "content"]
                }
            }
        }
    ]);

    loop {
        print!("> ");
        stdout().flush()?;
        let mut message = String::new();
        stdin().read_line(&mut message)?;
        if message.trim() == "exit" {
            break;
        }
        messages.push(json!({"role": "user", "content": message.trim()}));

        loop {
            let resp = client
                .post("https://api.groq.com/openai/v1/chat/completions")
                .bearer_auth(&key)
                .json(&json!({
                    "model": "openai/gpt-oss-120b",
                    "messages": messages,
                    "tools": tools,
                    "stream": true
                }))
                .send()
                .await?;

            let mut stream = resp.bytes_stream().eventsource();
            let mut calls: Vec<ToolCall> = vec![];
            let mut response_msg = String::new();

            while let Some(ev) = stream.next().await {
                let ev = ev?;
                if ev.data == "[DONE]" {
                    break;
                }
                let v: Value = serde_json::from_str(&ev.data)?;
                let delta = &v["choices"][0]["delta"];
                if let Some(t) = delta["content"].as_str() {
                    response_msg.push_str(t);
                    print!("{t}");
                    stdout().flush()?;
                }
                if let Some(tcs) = delta["tool_calls"].as_array() {
                    for tc in tcs {
                        let i = tc["index"].as_u64().unwrap_or(0) as usize;
                        if calls.len() <= i {
                            calls.resize_with(i + 1, Default::default);
                        }
                        if let Some(s) = tc["id"].as_str() {
                            calls[i].id = s.into();
                        }
                        if let Some(s) = tc["function"]["name"].as_str() {
                            calls[i].name = s.into();
                        }
                        if let Some(s) = tc["function"]["arguments"].as_str() {
                            calls[i].args.push_str(s);
                        }
                    }
                }
            }
            println!();

            let mut msg = json!({"role": "assistant", "content": response_msg});
            if !calls.is_empty() {
                let tcs: Vec<Value> = calls
                    .iter()
                    .map(|c| {
                        json!({
                            "id": c.id,
                            "type": "function",
                            "function": {"name": c.name, "arguments": c.args}
                        })
                    })
                    .collect();
                msg["tool_calls"] = json!(tcs);
            }
            messages.push(msg);

            if calls.is_empty() {
                break;
            }

            for c in &calls {
                println!("[tool] {}({})", c.name, c.args);
                let out = run_tool(&c.name, &c.args);
                messages.push(json!({
                    "role": "tool",
                    "tool_call_id": c.id,
                    "content": out
                }));
            }
        }
    }

    Ok(())
}
