# QSAutomate

Orchestration platform for algorithmic trading strategies with functionality for workflow automation, task scheduling, monitoring, and deployment.

![tmp2dxb9bjw](https://github.com/user-attachments/assets/9e5b4e1b-ca89-4632-9c4f-8a0442c52e78)

# 🚨 Disclaimer

This software is for educational purposes only. Use your paper trading account. Do not risk money which you are afraid to lose. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.

# ⬆️ Installation

## Install the latest with pip:

> Note: When you install QSAutomate, all these files will be hidden in your installation path. You may want to consider simply downloading the package and using as a base to expand from rather that as a library.

``` bash
pip install git+https://github.com/quant-science/QSAutomate.git
```

### Method 1: Using a Personal Access Token (PAT)

1. **Generate a Personal Access Token**  
   - Log into GitHub.  
   - Go to **Settings** → **Developer Settings** → **Personal access tokens** → **Tokens (classic)** (or **Fine-grained tokens**).  
   - Click **Generate new token**.  
   - Grant the token at least the **repo** scope (for private repos) or the relevant fine-grained access permissions.

2. **Use the Token in Your `pip install` Command**  
   - Replace `<GITHUB_USERNAME>` and `<PERSONAL_ACCESS_TOKEN>` with your actual username and the token string:
     ```bash
     pip install git+https://<GITHUB_USERNAME>:<PERSONAL_ACCESS_TOKEN>@github.com/quant-science/QSAutomate.git
     ```
   - This embeds your credentials securely (as a token rather than a password).

3. **Verify Repo Permissions**  
   - Make sure you have been granted access to the private repository.  
   - If you see a 404 or 403 error, confirm the token is valid and has the right scopes.

### Method 2: Using SSH Keys

1. **Generate SSH Keys (If Needed)**  
   - On your local machine, generate a key pair (if you don’t already have one):
     ```bash
     ssh-keygen -t ed25519 -C "your_email@example.com"
     ```
   - Copy the public key (e.g., from `~/.ssh/id_ed25519.pub`) to your GitHub **SSH and GPG keys** settings.

2. **Install via SSH**  
   - Use the SSH URL in your `pip install` command:
     ```bash
     pip install git+ssh://git@github.com/quant-science/omega.git
     ```
   - No token or password required.

# 💸 Paper Trading Account

**Important: Make sure to use a paper trading account to trade risk-free.** 

Here's how to find you account number from inside IB Trader Workstation. 

![image](https://github.com/quant-science/omega/assets/13734662/eba33283-ec31-4287-944f-4e9dff7cbe14)

# 🏃 Quick start

If you know what you're doing, navigate to `qsautomate/trading` and run the following command:

```
python qsmomentum.py
```

If you get an error, read on.

# 🏁 Getting Set up

## Running MLFlow

MLFlow is the tool we use to track experiments. QSAutomate uses QSResearch to automate the end to end research process. As such, we want to make sure MLFlow is running to collect strategy diagnostics. The easiest way to start MLFlow is through the command line. Make sure you have your Quant Lab activiated and MLFlow installed.

To point to a common MLFlow directory where artifacts are stored (recommended):

```bash
mlflow server \
  --port 8031 \
  --backend-store-uri ~/.qsresearch/mlflow/runs \
  --default-artifact-root ~/.qsresearch/mlflow/artifacts
```

This will start MLFlow and point to the directories based on the instructions for QSResearch. You will see some logging print out including the URL and port where the MLFlow server is listenting. If you followed the above instructions, it's here:

`[http://127.0.0.1:8031](http://127.0.0.1:8031)`

[You can find more options here.](https://mlflow.org/docs/latest/api_reference/cli.html#mlflow-server)

## Running Prefect

[Prefect](https://docs.prefect.io/v3/get-started) is an open-source orchestration engine that turns your Python functions into production-grade data pipelines with minimal friction. You can build and schedule workflows in pure Python—no DSLs or complex config files—and run them anywhere you can run Python. Prefect handles the heavy lifting for you out of the box: automatic state tracking, failure handling, real-time monitoring, and more.

Start the local Prefect server in a new terminal window with your Quant Stack activated:

```
prefect server start
```

This will start Prefect. You will see some logging preint out including the URL and port where the Prefect server is listening. If you followed the above instructions, it's here:

`[http://127.0.0.1:4200/](http://127.0.0.1:4200/)`

## Viewing and cancelling _flows_

You can review all flow runs in the UI available at the link above. If you want to cancel a _flow_ run, I find using the CLI is the most efficient.

Step 1. List all flow runs.

```prefect flow-run ls```

You'll see a nice table print out with the ID, Flow, Name, State, and when it started. Copy the ID of the flow you want to cancel.

Step 2. Cancel the flow run.

```prefect flow-run cancel <FLOW_RUN_ID>```

Prefect will schedule the flow run for cancellation.

# 💵 Using QSAutomate

QSAutomate is actually a collection of _tasks_ and _flow_ that together orchestrate our end to end trading strategy. Here are the important directories:

```
QSAutomate/                                   - Project root
├─ .env.example                                - Example environment variables
├─ qsautomate/                                 - Main Python package
│  ├─ backtest/                                - Backtesting runners/integration
│  │  ├─ __init__.py                           - Subpackage marker
│  │  └─ zipline_runner.py                     - Zipline-based backtest launcher
│  ├─ data/                                    - Data ingestion and bundling
│  │  ├─ bundle.py                             - Zipline data bundle registration/IO
│  │  └─ fmp.py                                - Prefect tasks to fetch/store FMP data
│  ├─ strategies/                              - Strategy definitions and artifacts
│  │  ├─ __init__.py                           - Subpackage marker
│  │  ├─ qsmomentum.py                         - Momentum strategy config/entrypoint
│  ├─ trading/                                 - Live/exec utilities and portfolio ops
│  │  ├─ __init__.py                           - Subpackage marker
│  │  └─ rebalance.py                          - Portfolio rebalancing logic
└─
```

> Note: When you install QSAutomate, all these files will be hidden in your installation path. You may want to consider simply downloading the package and using as a base to expand from rather that as a library.

QSAutomate is designed to be run at the command line. The entry point are the files in the `strategies` folders (if you want to follow the convention). These files define the Prefect _flow_ which actually orchestrates the _tasks_.