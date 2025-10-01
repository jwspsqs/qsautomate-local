# QSAutomate

Orchestration platform for algorithmic trading strategies with functionality for workflow automation, task scheduling, monitoring, and deployment.

<img width="1631" height="1315" alt="Screenshot 2025-08-11 at 08 54 31" src="https://github.com/user-attachments/assets/1abed6ca-67f6-41e0-941d-c14abc9577c3" />

# 🚨 Disclaimer

This software is for educational purposes only. Use your paper trading account. Do not risk money which you are afraid to lose. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.

# ⬆️ Installation

## Clone the repo and install the dependencies

> Note: QSAutomate is not designed to be installed like other libraries. It is a collection of files and directories you can use as the foundation for your algorithmic trading strategies. This makes installation even easier.

``` bash
git clone https://github.com/quant-science/QSAutomate.git
```

Then navigate into the `QSAutomate` directory (which is now your working directory) and install the dependencies:

```
pip install -r requirements.txt
```
You can now use the `QSAutomate` directory as your working directory.

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

[http://127.0.0.1:8031](http://127.0.0.1:8031)

[You can find more options here.](https://mlflow.org/docs/latest/api_reference/cli.html#mlflow-server)

## Running Prefect

[Prefect](https://docs.prefect.io/v3/get-started) is an open-source orchestration engine that turns your Python functions into production-grade data pipelines with minimal friction. You can build and schedule workflows in pure Python—no DSLs or complex config files—and run them anywhere you can run Python. Prefect handles the heavy lifting for you out of the box: automatic state tracking, failure handling, real-time monitoring, and more.

Start the local Prefect server in a new terminal window with your Quant Stack activated:

```
prefect server start
```

This will start Prefect. You will see some logging preint out including the URL and port where the Prefect server is listening. If you followed the above instructions, it's here:

[http://127.0.0.1:4200/](http://127.0.0.1:4200/)

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
│  │  └─ zipline_runner.py                     - Zipline-based backtest launcher
│  ├─ data/                                    - Data ingestion and bundling
│  │  ├─ bundle.py                             - Zipline data bundle registration/IO
│  │  └─ fmp.py                                - Prefect tasks to fetch/store FMP data
│  ├─ strategies/                              - Strategy definitions and artifacts
│  │  ├─ qsmomentum.py                         - Momentum strategy config/entrypoint
│  ├─ trading/                                 - Live/exec utilities and portfolio ops
└─ -  └─ rebalance.py                          - Portfolio rebalancing logic
```

QSAutomate is designed to be run at the command line. The entry point are the files in the `strategies` folders (if you want to follow the convention). These files define the Prefect _flow_ which actually orchestrates the _tasks_.

# 🔥 Using QSAutomate with QuantVPS

QuantVPS is a Virtual Private Server (VPS) provider designed specifically for algorithmic and high-frequency trading. Their servers are colocated near the Chicago Mercantile Exchange (CME), which helps reduce latency and slippage when trading futures. QuantVPS also provides trading-focused features such as NVMe storage, high-performance CPUs, and 24/7 support, with compatibility for platforms like NinjaTrader, MetaTrader, and TradeStation.

We’ve partnered with QuantVPS to provide complete, ready-to-use environments so you can run your algorithmic trading strategies without setup headaches.

Our students enjoy a 15% discount on any VPS for life.

When you sign up, your virtual private server comes preinstalled with:

1. Trader Workstation (version 10.40 Stable)
2. Visual Studio Code (VSCode) with the Dev Containers extension  
3. Docker Desktop (Docker will start when you log in)
4. Git for Windows (configured through the `bash` shell)

All apps can be accessed through links on the desktop.

In addition to a fully configured trading environment, you can run your strategies inside **Dev Containers**.

Dev Containers are portable development environments defined by a configuration file (`devcontainer.json`). This file specifies the tools, runtimes, extensions, and settings you need. Dev Containers run inside Docker, ensuring consistent environments across machines and teams. This removes the classic “works on my machine” problem by standardizing dependencies and workflows.

That means it’s easier than ever to get the **Quant Scientist Stack** running with all required Python libraries:

1. No more dependency issues  
2. No more GitHub login problems  
3. No more outdated libraries  
4. No more mismatched Python versions  
5. No more missing system packages (like libxml, gcc, etc.)  
6. No more inconsistent IDE setups—extensions and settings are shared  
7. No more “works on Linux but not on Windows/Mac” problems  
8. No more manual environment setup—everything is defined in code  

### Setup Steps

1. [Log into](https://www.quantvps.com/login) your QuantVPS virtual private server.  
2. Set up your [GitHub Personal Access Token](https://docs.github.com/en/authentication/keeping-your-account-and-data-secure/managing-your-personal-access-tokens#creating-a-personal-access-token-classic) (PAT) and **copy it**.  
3. Open **Git Bash** from the desktop shortcut and clone the `QSAutomate` repository to the desktop:  

```bash
$ cd Desktop
$ git clone https@github.com:quant-science/QSAutomate.git
```

When prompted, complete the sign in process.  

4. Open the `QSAutomate` folder in VSCode.  
5. Rename `.env.example` to `.env`.  
6. Add your FMP API key and GitHub Personal Access Token to the `.env` file.  
7. Open the Dev Container in VSCode.  

VSCode will automatically detect the `devcontainer.json` file and ask that you open the current directory remotely. If you do not see this option, click the little blue `><` icon in the bottom left corner of the window. From there, select Open Folder in Container from the Command Pallet. Once selected, open the `QSAutomate` directory from the popup and the `devcontainer.json` file from the COmmand Pallet.

The first time you do this, it will take about five minutes to set everything up. After that it takes about five seconds.

At this point, the entire Quant Scientist Stack is installed and ready to use.

This includes:

- Omega  
- QSConnect  
- QSResearch  
- Zipline Reloaded  
- Pyfolio Reloaded  
- Alphalens Reloaded  
- XGBoost  
- and more...  

---

## Running Omega

When using Omega, Docker networking automatically forwards the connection to the instance of Trader Workstation running on the host. This means you’ll need to create your Omega trading apps using the internal hostname defined during the container build.

It sounds complex, but it’s simple in practice. Use this pattern:

```python
>>> import omega
>>> app = omega.Omega(host="host.docker.internal")
```

Before creating your Omega app, make sure Trader Workstation is running on the `host` computer. (In other words, double click the TWS icon on the desktop and sign in.)

With this setup, Omega seamlessly connects to Trader Workstation through Docker, letting you focus on building and testing your trading strategies.
