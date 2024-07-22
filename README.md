# Discord Channel Bridge Bot
This bot is a simple Python bot for bridging messages between Discord text channels, both within a server and across multiple servers.

### Usage
To add this bot to your server, visit [this link](https://discord.com/oauth2/authorize?client_id=1253380419773136947) and invite it. From there, usage is simple: just use the `/bridge target` slash-command and a bridge between the current and target channels will be created! Messages, attachments, reactions, edits, and deletions will be mirrored, and both channels will act as if they are the same.

There are, however, many more commands than just that, as well as some options for customisation. Here are some details:

- You can use the `/bridge target` command to and from either a text channel or a (public) thread off one.
  - `target` can be either a Discord link (i.e. `https://discord.com/channels/server_id/channel_or_thread_id`) or a channel/thread mention (i.e. `<#channel_or_thread_id>`).
  - You can instead use the command `/bridge target outbound` to create an outbound-only bridge—that is, one in which only messages from the current channel/thread are sent to the other channel/thread and not vice-versa—or `/bridge target inbound` to create an inbound-only bridge—the converse, the current channel/thread will receive but not send messages.
  - The bot needs to be in the channel/thread that is receiving messages and it and you need to have Manage Webhooks permissions in it.
  - It's possible to create channel-thread bridges, not just channel-channel and thread-thread ones.
  - You don't need to create bridges between every channel/thread pair involved, as message bridging goes down outbound bridge chains—that is, if there is an outbound bridge from `#A` to `#B` and an outbound bridge from `#B` to `#C`, messages sent to `#A` will be mirrored in both `#B` and `#C`.
    - This does not, however, actually create a bridge from `#A` to `#C`. If, for example, the bridge from `#A` to `#B` is destroyed, messages from `#A` will no longer be sent to `#C`.
    - That said, the bot deals with multiple possible bridge configurations gracefully: having an `#A` -> `#B` -> `#C` sequence and an `#A` -> `#C` bridge will not duplicate messages sent from `#A` in `#C`, nor will a `#A` -> `#B` -> `#C` -> `#A` cycle, and the latter will, in practice, be equivalent to all three channels having two-way bridges between each other.
- Messages, attachments, reactions, edits, and deletions are bridged. A webhook mimics the original author's name and profile picture when doing so, trying to match their details across the bridge if they are also present there, and using their public ones if they are not.
  - It will not mirror thread creation; if you want threads in bridged channels to be mirrored, you need to run the bridge command from within them.
    - Alternatively, you can run the command `/bridge_thread` from within a thread and it will try to create threads in all channels with inbound bridges from the parent channel following the same bridge rules present in it.
    - Another option is running the command `/auto_bridge_threads` from a channel; this will cause thread creation in that channel to be mirrored (though you'll need to run that command on each side of each bridge for automatic thread creation to work in all directions). Running this command again will revert to the default behaviour.
  - When bridging a reaction emoji, if the bot doesn't have access to the emoji being bridged, it will attempt to copy that emoji into an emoji server and then react with the copied emoji.
- You can run the command `/demolish target` to demolish all bridges between the current and target channels/threads.
  - You can run the command `/demolish_all` to demolish all bridges to and from the current channel/thread (and, optionally, its threads or its parent channel's threads).
- Messages sent from most other bots or webhooks are not bridged; the only current global exception is [Tupperbox](https://tupperbox.app/). However, it is possible to add local exceptions—that is, bots whose messages will be bridged from a given specific channel—by running the command `/whitelist @bot [@bot_2 [@bot_3 ...]]`.
  - This command only works in channels with outbound bridges coming from them, and only bots that are in the channel can be added this way.
  - Sequential bridges will work: if `#A` -> `#B` -> `#C` and a bot is whitelisted in `#A`, its messages there will be bridged to `#B` and `#C`. However, if that bot sends a message in `#B` and it is not whitelisted there, the message will not be bridged to `#C`.
  - Running the command again removes the application(s) from the whitelist.
- `/help` will give you a list of commands. `/help command_name` will explain the usage of the specific command passed as argument.
  - You can pass this command in DM to the bot, too.
- Right clicking a message and going to Apps > List Reactions will show you a list of all reactions to that message as well as the users who added each reaction for all versions of that message visible from the current channel.

### Running your own copy
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
