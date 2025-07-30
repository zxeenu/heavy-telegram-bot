# Gateway

## Setting Up

### Environment Variables

Before starting, create a `.env` file in the root of the project with your Telegram API credentials.

#### 🔑 Getting Keys

- **Telegram API ID & Hash:**  
  Obtain these from [Telegram's official docs](https://core.telegram.org/api/obtaining_api_id).

```env
# .env example
TELEGRAM_ID=your_telegram_api_id
TELEGRAM_HASH=your_telegram_api_hash
```

## Development Notes

This project uses Visual Studio Code Dev Containers for a seamless development experience. Once your `.env` is set, you should be good to go.

#### 🐞 Dev Container Permission Issues?

If your dev container starts acting up and you can't edit files, try fixing the file ownership:

```bash
sudo chown -R vscode:vscode .
```
