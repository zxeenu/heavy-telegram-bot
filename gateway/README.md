# Gateway

## Setting Up

### 📦 Environment Variables

Before starting, make sure to create a `.env` file with the required API keys.

#### 🔑 Getting Keys

- **Telegram API ID & Hash:**  
  Obtain these from [Telegram's official docs](https://core.telegram.org/api/obtaining_api_id).

```env
# .env example
TELEGRAM_ID=your_telegram_api_id
TELEGRAM_HASH=your_telegram_api_hash
```

## 🛠️ Notes

If your dev container starts acting up and you can't edit files, try fixing permissions:

```bash
sudo chown -R vscode:vscode .
```

Just paste that into your Markdown file and you're good to go.
