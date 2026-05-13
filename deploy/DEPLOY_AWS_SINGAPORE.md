# Deploy to AWS Singapore (ap-southeast-1)

Deploying close to Binance servers (Tokyo/Singapore) reduces WS frame delivery from ~100-300ms (India) to ~1-5ms.

## Prerequisites

- AWS account with access to `ap-southeast-1` region
- Docker installed locally
- AWS CLI configured (`aws configure`)

## Option A: EC2 (simplest, lowest latency)

### 1. Launch EC2

```bash
# t3.small is enough (2 vCPU, 2GB RAM)
aws ec2 run-instances \
  --region ap-southeast-1 \
  --image-id ami-0c55b159cbfafe1f0 \
  --instance-type t3.small \
  --key-name your-key \
  --security-group-ids sg-xxxx \
  --tag-specifications 'ResourceType=instance,Tags=[{Key=Name,Value=crypto-announcements}]'
```

Security group: allow outbound 443 (Binance API/WS), inbound 22 (SSH).

### 2. Install Docker on EC2

```bash
sudo yum update -y
sudo yum install -y docker
sudo systemctl start docker
sudo usermod -a -G docker ec2-user
```

### 3. Build and push image

```bash
# Build locally
docker build -t crypto-announcements .

# Tag for ECR or push to Docker Hub
docker tag crypto-announcements:latest YOUR_REGISTRY/crypto-announcements:latest
docker push YOUR_REGISTRY/crypto-announcements:latest
```

### 4. Run on EC2

```bash
# Pull and run
docker pull YOUR_REGISTRY/crypto-announcements:latest

docker run -d --restart=always \
  --name crypto-server \
  --env-file /home/ec2-user/.env \
  YOUR_REGISTRY/crypto-announcements:latest
```

### 5. PostgreSQL

Option 1: RDS PostgreSQL in same region (ap-southeast-1), ~1-2ms latency.
Option 2: Run Postgres on the same EC2 instance (0ms network, simpler).

```bash
# Same-instance Postgres (docker)
docker run -d --name pg \
  -e POSTGRES_PASSWORD=postgres \
  -e POSTGRES_DB=crypto_announcements_development \
  -p 5432:5432 \
  --restart=always \
  postgres:16-alpine
```

Update `.env`: `DATABASE_URL=postgres://postgres:postgres@localhost:5432/crypto_announcements_development?sslmode=disable`

### 6. .env on server

Copy your local `.env` to the EC2 instance:
```bash
scp -i your-key.pem .env ec2-user@<EC2_IP>:/home/ec2-user/.env
```

## Option B: ECS Fargate (managed, auto-restart)

```bash
# Create ECR repo
aws ecr create-repository --repository-name crypto-announcements --region ap-southeast-1

# Build, tag, push
aws ecr get-login-password --region ap-southeast-1 | docker login --username AWS --password-stdin ACCOUNT.dkr.ecr.ap-southeast-1.amazonaws.com
docker build -t crypto-announcements .
docker tag crypto-announcements:latest ACCOUNT.dkr.ecr.ap-southeast-1.amazonaws.com/crypto-announcements:latest
docker push ACCOUNT.dkr.ecr.ap-southeast-1.amazonaws.com/crypto-announcements:latest
```

Then create ECS task definition + service via AWS console or Terraform.

## Expected latency after deployment

| Metric | India | Singapore |
|--------|-------|-----------|
| WS announcement frame | 100-300ms | 1-5ms |
| POST /fapi/v1/order | 50-200ms | 5-15ms |
| Total (announcement → trade) | 200-500ms | 10-25ms |

## Monitoring

```bash
# View logs
docker logs -f crypto-server

# Check if running
docker ps
```
