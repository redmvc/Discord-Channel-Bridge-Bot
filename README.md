# Discord Channel Bridge Bot
This bot is a simple Python bot for bridging messages between Discord text channels, both within a server and across multiple servers.

### Usage
To add this bot to your server, visit its website (under construction) and invite it. From there, usage is simple:

- From the dashboard on the site, anyone with Administrator permissions in the server will be able to set the command prefix. The default prefix is `:`.
- Then, within a text channel or thread, you can use the `:bridge target` command to create a two-way bridge between the channel or thread you are currently typing in and the target.
  - `target` can be either a Discord link (i.e. `https://discord.com/channels/server_id/channel_or_thread_id`) or a channel mention (i.e. `<#channel_or_thread_id>`).
  - You can instead use the command `:outbound target` to create an outbound-only bridge—that is, one in which only messages from the current channel are sent to the other channel and not vice-versa—or `:inbound target` to create an inbound-only bridge—the converse, the current channel will receive but not send messages.
  - The bot needs to be in both channels and it and you need to have Manage Webhooks permissions in both channels.
  - It's possible to create channel-thread bridges, not just channel-channel and thread-thread ones.
  - You need to run this command for every channel pair involved. That is, if you want to bridge channels A, B, and C, you will need to run `:bridge B` and `:bridge C` from channel A, then `:bridge C` from channel B (or some other order).
- The bot will mirror messages, attachments, reactions, edits, and deletions.
  - It will not mirror thread creation; if you want threads in both channels to be mirrored, you need to run the bridge command from within them.
    - Alternatively, you can run the command `:bridge_thread` from within a new thread and it will try to create threads in all channels bridged to the current one following the same bridge rules present in the parent channel.
- You can run the command `:demolish target` to demolish all bridges between the current and target channels.
  - You can run the command `:demolish_all` to demolish all bridges to and from the current channel.
- `:help` will give you a list of commands. `:help command_name` will explain the usage of the specific command passed as argument.
- Using the command `:list_reacts message` (where `message` is a Discord link to a message) will make the bot DM you a list of all users who reacted to a given message on all sides of a bridge.
- You can DM the bot all of these commands.
  - `:bridge source target`, `:outbound source target`, `:inbound source target`, `:demolish source target`, and `:demolish_all source` are equivalent to running their matching commands from within channel source.

### Help, issues, and bugs

If you have any issues with this bot, feel free to create an issue on the Issue Tracker or DM `redmagnos` on Discord.

### License

This project is licensed under the GNU General Public License v3.0 License. See the LICENSE file for details.
