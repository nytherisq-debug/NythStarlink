# 🤖 Star Link Bot

Telegram bot for scanning Star Link access codes with proxy rotation.

## 🚀 Deploy on Railway

### 1. Prerequisites
- GitHub account
- Railway account (https://railway.app)
- Telegram Bot Token (from @BotFather)

### 2. Setup
1. Fork/clone this repo
2. Create new project on Railway → **Deploy from GitHub repo**
3. Add **Environment Variables**:
   - `BOT_TOKEN` = your bot token
   - `ADMIN_ID` = your telegram user ID
   - `ADMIN_CONTACT` = your telegram username
   - `DATA_DIR` = `/data`
4. Add **Volume**:
   - Settings → Volumes → New Volume
   - Mount path: `/data`
5. Deploy!

### 3. Commands
- `/start` - Start bot
- `/genkey <plan> <user_id> <limit>` - Generate key (admin)
- `/scan` - Start scanning
- `/stop` - Stop scanning
- `/result` - View results

## 📦 Local Development

```bash
pip install -r requirements.txt
cp .env.example .env
# Edit .env with your tokens
python bot.py