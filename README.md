# QSAutomate

Orchestration platform for algorithmic trading strategies with functionality for workflow automation, task scheduling, monitoring, and deployment.

![tmp2dxb9bjw](https://github.com/user-attachments/assets/9e5b4e1b-ca89-4632-9c4f-8a0442c52e78)

# 🚨 Disclaimer

This software is for educational purposes only. Use your paper trading account. Do not risk money which you are afraid to lose. USE THE SOFTWARE AT YOUR OWN RISK. THE AUTHORS AND ALL AFFILIATES ASSUME NO RESPONSIBILITY FOR YOUR TRADING RESULTS.

# ⬆️ Installation

## Install the latest with pip:

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
