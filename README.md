# Discord Channel Bridge Bot
This bot is a simple Python bot for bridging messages between Discord text channels, both within a server and across multiple servers.

## Key Features
- Mirror all messages, attachments, and embeds sent to a channel or thread to another channel or thread.
- Match reactions, edits, deletions, replies, and forwarding between bridged messages.
- Retrieve a list of all reactions to a given message on all sides of bridges to it.

## Usage
To add this bot to your server, visit [this link](https://discord.com/oauth2/authorize?client_id=1253380419773136947) and invite it. From there, usage is simple: just use the `/bridge target` slash-command and a bridge between the current and target channels will be created! Messages, attachments, reactions, edits, and deletions will be mirrored, and both channels will act as if they are the same.

There are, however, many more commands than just that, as well as some options for customisation. You can check the [documentation](placeholder) for details!

## Running your own copy
It's very straightforward to run your own copy of this bot. You'll need access to an SQL database running MySQL, PostgreSQL, or SQLite, and a Discord developer account.
1. Go to the [applications page](https://discord.com/developers/applications) on the Discord Developers platform and create a new application.
2. Under the "Bot" tab, make sure your bot has access to the "Server Members Intent" and the "Message Content Intent".
   - Grab the authorisation token from that page, too, and save it to store it in your settings file later.
3. Generate an install link under the "Installation" tab:
   - Use the "Guild Install" authorization method.
   - Add the `applications.commands` and `bot` scopes.
   - Add the `Add Reactions`, `Attach Files`, `Create Public Threads`, `Embed Links`, `Read Message History`, `Read Messages/View Channels`, `Send Messages`, `Send Messages in Threads`, `Use External Emojis`, `Use External Stickers`, and `Use Slash Commands` default permissions.
4. Create a `settings.json` file in the same folder as your `main.py` file with the following entries, filling them out with the appropriate values for your own application and server:
   ```json
   {
      "context": "production",
      "production": {
         "app_token": "the token you got in step 2",
         "db_dialect": "mysql, postgresql, or sqlite, depending on which dialect your database uses",
         "db_driver": "pymysql, psycopg2, or pysqlite, respectively depending on the above",
         "db_host": "",
         "db_port": 0,
         "db_user": "",
         "db_pwd": "",
         "db_name": ""
      }
   }
   ```
   - Optionally, you can add an `"emoji_server_id"` entry to that list. If this ID points to a valid Discord server to which the bot has `Create Expressions` and `Manage Expressions` permissions, that server will be used to add any custom reactions it runs into but doesn't have access to while trying to bridge reactions.
   - You can also add a `"whitelisted_apps"` entry with a list of IDs of apps to let through the bridge.
   - You may add other contexts than `"production"`, such as `"testing"`, for other situations.
5. Edit your `requirements.txt` file to include the appropriate SQL library depending on your SQL dialect, then run `pip install -r requirements.txt` on your command line from the main folder.
6. Run `main.py`. This will automatically create the necessary tables in your database if they're not already there, and all commands will be working out of the box.

If you set up a server for the bot to store emoji, you can run the command `/map_emoji :internal_emoji: :external_emoji: [:external_emoji_2: [:external_emoji_3: ...]]` from that server to create an internal equivalence between emoji for the bot to bridge. You can also run the command `/hash_server_emoji [server_id]` in order to store hashes of every emoji in a server so that the bridge will store hashes of each emoji so that it can automatically match different emoji from different servers that use the same image.

### Help, issues, and bugs
If you have any issues with this bot, feel free to create an issue on the Issue Tracker or DM `redmagnos` on Discord.

### License
This project is licensed under the GNU General Public License v3.0 License. See the LICENSE file for details.
