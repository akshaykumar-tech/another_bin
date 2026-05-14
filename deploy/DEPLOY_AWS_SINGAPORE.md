# AWS Singapore Deployment — Complete Step-by-Step Guide

This guide takes you from zero to a running crypto announcements server on AWS Singapore
(ap-southeast-1), close to Binance servers for minimum latency.

---

## Step 1: AWS Account Setup

If you don't have one already:
1. Go to https://aws.amazon.com and create an account
2. Add a payment method (credit/debit card)
3. Region select karo: **Asia Pacific (Singapore) ap-southeast-1**
   - Top-right corner mein region dropdown hai, woh change karo

---

## Step 2: Create SSH Key Pair

This lets you SSH into the EC2 instance.

1. AWS Console > **EC2** > Left sidebar > **Key Pairs**
2. Click **Create key pair**
   - Name: `crypto-key`
   - Type: **RSA**
   - Format: **.pem** (for Linux/Mac) or **.ppk** (for Windows PuTTY)
3. Download hoga — **isko safe rakho, dobara download nahi hoga**
4. Local machine par permissions set karo:
   ```bash
   chmod 400 ~/Downloads/crypto-key.pem
   ```

---

## Step 3: Create Security Group

This controls network access to your server.

1. AWS Console > **EC2** > **Security Groups** > **Create security group**
2. Settings:
   - Name: `crypto-server-sg`
   - Description: `Crypto announcements server`
   - VPC: default

3. **Inbound rules** (Add rule):

   | Type | Port | Source | Purpose |
   |------|------|--------|---------|
   | SSH | 22 | My IP | Your SSH access |

4. **Outbound rules** (default = All traffic allowed — keep as-is):
   - Server ko Binance WS (443), FAPI (443), Postgres (internal) sab access chahiye
   - Default "All traffic - 0.0.0.0/0" theek hai

5. Click **Create security group** — note the **sg-xxxxxxxx** ID

---

## Step 4: Launch EC2 Instance

### Recommended Configuration

| Setting | Value | Why |
|---------|-------|-----|
| **Region** | ap-southeast-1 (Singapore) | Closest to Binance servers |
| **AMI** | Amazon Linux 2023 (x86_64) | Lightweight, Docker-ready |
| **Instance type** | **t3.small** | 2 vCPU, 2GB RAM — enough for server + Postgres |
| **Storage** | 20 GB gp3 | Schema + logs fit easily |
| **Key pair** | crypto-key (from Step 2) | SSH access |
| **Security group** | crypto-server-sg (from Step 3) | Network rules |

### Cost estimate
- t3.small: ~$0.023/hr = ~$17/month (on-demand)
- gp3 20GB: ~$1.60/month
- **Total: ~$19/month**
- Spot instance use karo for ~$5-7/month (can be interrupted, but rare)

### Launch via Console

1. AWS Console > **EC2** > **Launch Instance**
2. **Name:** `crypto-announcements`
3. **AMI:** Search "Amazon Linux 2023" > Select the x86_64 one
4. **Instance type:** `t3.small`
5. **Key pair:** Select `crypto-key`
6. **Network settings:** Click Edit
   - Select existing security group: `crypto-server-sg`
7. **Storage:** Change to `20 GiB gp3`
8. Click **Launch instance**
9. Note the **Public IPv4 address** after launch (e.g. `13.212.xxx.xxx`)

### Launch via CLI (alternative)

```bash
# Find latest Amazon Linux 2023 AMI
AMI_ID=$(aws ec2 describe-images \
  --region ap-southeast-1 \
  --owners amazon \
  --filters "Name=name,Values=al2023-ami-2023*-x86_64" \
  --query 'Images | sort_by(@, &CreationDate) | [-1].ImageId' \
  --output text)

echo "Using AMI: $AMI_ID"

# Launch
aws ec2 run-instances \
  --region ap-southeast-1 \
  --image-id "$AMI_ID" \
  --instance-type t3.small \
  --key-name crypto-key \
  --security-group-ids sg-XXXXXXXX \
  --block-device-mappings '[{"DeviceName":"/dev/xvda","Ebs":{"VolumeSize":20,"VolumeType":"gp3"}}]' \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=crypto-announcements}]'
```

---

## Step 5: SSH into EC2

```bash
ssh -i ~/Downloads/crypto-key.pem ec2-user@<EC2_PUBLIC_IP>
```

If you get "Permission denied": make sure you did `chmod 400 crypto-key.pem`.

---

## Step 6: Install Docker + Docker Compose on EC2

```bash
# Update system
sudo dnf update -y

# Install Docker
sudo dnf install -y docker
sudo systemctl start docker
sudo systemctl enable docker
sudo usermod -a -G docker ec2-user

# Install Docker Compose plugin
sudo mkdir -p /usr/local/lib/docker/cli-plugins
sudo curl -SL "https://github.com/docker/compose/releases/latest/download/docker-compose-linux-x86_64" \
  -o /usr/local/lib/docker/cli-plugins/docker-compose
sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose

# Verify (re-login for group change)
exit
```

SSH again:
```bash
ssh -i ~/Downloads/crypto-key.pem ec2-user@<EC2_PUBLIC_IP>
docker --version
docker compose version
```

---

## Step 7: Copy Project to EC2

From your **local machine** (not EC2):

```bash
cd ~/Desktop/binance/crypto_announcements_go

# Copy entire project
scp -r -i ~/Downloads/crypto-key.pem \
  . ec2-user@<EC2_PUBLIC_IP>:~/app/
```

Or use git:
```bash
# On EC2:
sudo dnf install -y git
git clone <your-repo-url> ~/app
cd ~/app
```

---

## Step 8: Create .env on EC2

```bash
ssh -i ~/Downloads/crypto-key.pem ec2-user@<EC2_PUBLIC_IP>
cd ~/app
nano .env
```

Paste this (fill in your real keys):

```env
DATABASE_URL=postgres://postgres:postgres@db:5432/crypto_announcements_development?sslmode=disable

ANNOUNCEMENT_SERVICE_MODE=all

BINANCE_WS_ENABLED=true
BINANCE_CMS_WS_BASE_URL=wss://api.binance.com/sapi/wss
BINANCE_CMS_TOPIC=com_announcement_en
BINANCE_API_KEY=<YOUR_REAL_BINANCE_API_KEY>
BINANCE_API_SECRET=<YOUR_REAL_BINANCE_API_SECRET>

UPBIT_ANNOUNCEMENTS_API_URL=https://api-manager.upbit.com/api/v1/announcements
UPBIT_POLL_INTERVAL_SECONDS=7
UPBIT_ANNOUNCEMENTS_API_PER_PAGE=1
UPBIT_ANNOUNCEMENTS_ONLY_LATEST=true

AUTO_TRADING_ENABLED=true
AUTO_TRADING_DRY_RUN=false
AUTO_TRADING_RECENT_MOVE_FILTER_ENABLED=false
AUTO_TRADING_RECENT_MOVE_LOOKBACK_SECONDS=20
AUTO_TRADING_RECENT_MOVE_SKIP_PERCENT=10
AUTO_TRADING_ULTRA_FAST_FIXED_MARGIN_USDT=20
```

Save: `Ctrl+O` > Enter > `Ctrl+X`

IMPORTANT: `DATABASE_URL` mein `@db:5432` rakho (not localhost) — Docker Compose internal networking.

---

## Step 9: Start Everything

```bash
cd ~/app
docker compose up -d
```

This will:
1. Build the Go server image (~1-2 min first time)
2. Start Postgres 16 container
3. Auto-load schema.sql + seed_data.sql into Postgres
4. Start server container, connect to DB
5. Open WebSocket to Binance for announcements

---

## Step 10: Verify

```bash
# Check both containers are running
docker compose ps

# Expected output:
#  NAME         SERVICE   STATUS
#  app-db-1     db        running (healthy)
#  app-server-1 server    running

# Check server logs
docker compose logs -f server

# Expected: you should see
# [binance_ws] command response: code=00000000 data="SUCCESS"
```

If you see errors:
- `connection refused db:5432` → DB not ready yet, wait 5 seconds, server will auto-retry
- `missing BINANCE_API_KEY` → .env mein keys fill nahi kiye
- `SQLSTATE 42P01 relation does not exist` → schema.sql load nahi hua; run: `docker compose down -v && docker compose up -d`

---

## Step 11: Test Latency

```bash
# From EC2, check ping to Binance
curl -o /dev/null -s -w "Connect: %{time_connect}s\nTotal: %{time_total}s\n" \
  https://fapi.binance.com/fapi/v1/time

# Expected from Singapore:
# Connect: 0.005s
# Total: 0.010s
#
# From India it would be:
# Connect: 0.100s
# Total: 0.200s
```

---

## Common Operations

```bash
# View live logs
docker compose logs -f server

# Restart server (after .env change)
docker compose restart server

# Rebuild after code change
docker compose up -d --build server

# Stop everything
docker compose down

# Stop + delete DB data (fresh start)
docker compose down -v

# Check DB manually
docker compose exec db psql -U postgres crypto_announcements_development

# Check trading_settings
docker compose exec db psql -U postgres crypto_announcements_development \
  -c "SELECT enabled, max_tokens_to_trade, announcement_actions FROM trading_settings;"

# Check recent announcements
docker compose exec db psql -U postgres crypto_announcements_development \
  -c "SELECT id, title, announcement_type, affected_tokens FROM announcements ORDER BY id DESC LIMIT 5;"

# Check trade executions
docker compose exec db psql -U postgres crypto_announcements_development \
  -c "SELECT id, symbol, position_side, status, entry_price, created_at FROM trade_executions ORDER BY id DESC LIMIT 10;"
```

---

## Auto-restart on EC2 reboot

Docker containers already have `restart: always` in docker-compose.yml. But Docker service itself
also needs to auto-start:

```bash
sudo systemctl enable docker
```

Now if EC2 reboots, Docker starts → containers restart automatically.

---

## Cost Optimization (optional)

| Option | Cost/month | Trade-off |
|--------|-----------|-----------|
| t3.small on-demand | ~$19 | Always available, simplest |
| t3.small spot | ~$6 | Can be interrupted (rare in Singapore) |
| t3.micro (1 vCPU, 1GB) | ~$10 | Tight on RAM with Postgres, but works |
| Free tier t2.micro | $0 (12 months) | 1GB RAM, might be tight but worth trying |

For spot instance:
```bash
aws ec2 run-instances \
  --region ap-southeast-1 \
  --instance-market-options '{"MarketType":"spot","SpotOptions":{"SpotInstanceType":"persistent"}}' \
  # ... rest same as Step 4
```

---

## Architecture Summary

```
Your Local Machine                    AWS Singapore (ap-southeast-1)
                                      ┌─────────────────────────────┐
                                      │  EC2 t3.small               │
 SSH/SCP ──────────────────────────── │                             │
                                      │  ┌─────────┐  ┌──────────┐ │
                                      │  │ server  │──│ postgres │ │
                                      │  │ (Go)    │  │ (Docker) │ │
                                      │  └────┬────┘  └──────────┘ │
                                      │       │                     │
                                      └───────┼─────────────────────┘
                                              │ ~5ms
                                      ┌───────┴───────┐
                                      │ Binance APIs  │
                                      │ WS + FAPI     │
                                      │ (Tokyo/SG)    │
                                      └───────────────┘
```
