Commands
==========

These are the commands available to users in channels the bot is in.

/bridge
------------------------------
Create a bridge between the current channel and the target channel, which will copy messages sent to one channel in the other. Editing or deleting the original message will also edit or delete its bridged versions, and adding reactions to a message will make the bot add that same reaction to its bridged versions. Both you and the bot need to have Manage Webhooks permission in channels that will be receiving messages.

By default bridges are two-way, so that messages sent from one channel are copied to the other and vice-versa. However, you can use the ``direction`` parameter to make them only one-way (i.e. only from the current to the target channel or only from the target to the current channel).

You don't need to create bridges between every channel/thread pair involved, as message bridging goes down outbound bridge chains—that is, if there is an outbound bridge from **#A** to **#B** and an outbound bridge from **#B** to **#C**, messages sent to **#A** will be mirrored in both **#B** and **#C**. This does not, however, actually create a bridge from **#A** to **#C**. If, for example, the bridge from **#A** to **#B** is destroyed, messages from **#A** will no longer be sent to **#C**.

.. note::
    The bot deals with multiple possible bridge configurations gracefully: having an **#A** -> **#B** -> **#C** sequence and an **#A** -> **#C** bridge will not duplicate messages sent from **#A** in **#C**, nor will a **#A** -> **#B** -> **#C** -> **#A** cycle, and the latter will, in practice, be equivalent to all three channels having two-way bridges between each other.

**Syntax:** ``/bridge target [direction]``

- ``target``
    The channel or thread to bridge the current channel or thread to. It can be a mention (``<#channel_or_thread_id>``), a link to it (``https://discord.com/channels/server_id/channel_or_thread_id``), or just its numerical ID.
- ``direction``
    Optionally, a single direction (``outbound`` or ``inbound``) for the bridge to be created. Not including this parameter will make it be two-way.
    
    Both of these do the same thing: if you run the command with parameter ``outbound`` from the current channel that is the same as running it with parameter ``inbound`` from the target channel.


/bridge_thread
------------------------------
By default, if a thread is created in a channel that is bridged to others, the bridges won't have a matching bridged thread created in them. If you run this command from within a new thread, the bot will attempt to create matching threads in channels bridged to its parent. Once created, they will be automatically bridged to the thread this command was called from.

It will only do so across outbound bridges: if the new thread's parent has an inbound bridge from channel **#A** but not an outbound bridge to it, a matching bridged thread will not be created there.

.. note::
    If the new thread's parent channel is itself bridged to *threads*, the new thread will not be mirrored across those particular bridges.

**Syntax:** ``/bridge_thread``


/auto_bridge_threads
------------------------------
If you run this command from within a channel that has at least one outbound bridge coming from it, new threads created in that channel will thenceforth be automatically bridged as if you were running ``/bridge_thread`` from all of them. You will need to run this command from each channel you want this to happen to, though; if **#A** has a two-way bridge to **#B** and you run this command from **#A**, threads created in **#A** will spawn new ones in **#B** but not vice-versa.

Running this command a second time will disable auto-thread-bridging.

**Syntax:** ``/auto_bridge_threads``


/demolish
------------------------------
Demolish all bridges (inbound and outbound) between the current or thread and the target channel or thread.

.. important::
    Demolishing a bridge will also break the connections between already-bridged messages, so that new edits, reactions, or deletions will no longer be bridged. This will remain the case even if the bridges are reconstituted later.

**Syntax:** ``/demolish target``

- ``target``
    The channel or thread to demolish bridges to and from. It can be a mention (``<#channel_or_thread_id>``), a link to it (``https://discord.com/channels/server_id/channel_or_thread_id``), or just its numerical ID.


/demolish_all
-----------------------------------------
Demolish all bridges to and from the current channel or thread.

**Syntax:** ``/demolish_all [channel_and_threads]``

- ``channel_and_threads``
    Optional. If set to ``True`` and the command is run from a channel, bridges to and from all of the channel's threads will also be demolished; if run from a thread, bridges to and from the thread's parent and all of the thread's parent's other threads will also be demolished.


/whitelist
-------------------------------------------------
By default, only messages from users and from the [Tupperbox](https://tupperbox.app/) bot are bridged. If you run this command, you can add one or more bots to the current thread or channel's whitelist, allowing their messages to also be bridged.

Running this command again removes the bot from the whitelist.

.. note::
    The bot(s) must be in the channel for this to work. Furthermore, this only works for outbound messages; in order to allow a bot through a two-sided bridge you need to run this command from both sides.

    Sequential bridges will work, though: if **#A** -> **#B** -> **#C** and a bot is whitelisted in **#A**, its messages there will be bridged to **#B** and **#C**. However, if that bot sends a message in **#B** and it is not whitelisted there, the message will not be bridged to **#C**.

**Syntax:** ``/whitelist bot [bot_2 [bot_3 ...]]``

- ``bot``
    A space-separated list of bot mentions (**@bot**) or bot user IDs to add to or remove from the whitelist. Note that the toggle is individual, so if you run ``/whitelist @bot1 @bot2 @bot3`` and **@bot1** was already whitelisted, this will remove **@bot1** from the whitelist and add **@bot2** and **@bot3** to it.


/map_emoji
----------------------------------------------------------------------------------------------
Create an internal map between an emoji in the emoji server and emoji from other servers, such that whenever the bot bridges one of the external emoji from the list it doesn't already have access to it will use ``internal_emoji`` instead.

.. note::
    Whenever the bot encounters an emoji it doesn't have access to, it automatically tries to match that emoji to another emoji it does have access to using the MD5 hash of the emoji's image and, if it can't, it tries to copy that emoji into its emoji server and use that copy instead. The main purpose of this command is matching emoji that are similar but not identical.

.. important::
    This command can only be run from the bot's special emoji server and only if you have Create Expressions and Manage Expressions permissions in it.

**Syntax:** ``/map_emoji internal_emoji external_emoji [external_emoji_2 ...]``

- ``internal_emoji``
    The emoji in the emoji server that you want to map other emoji to.
- ``external_emoji``
    A space separated list of emoji from other servers you want to match to the internal one.


/hash_server_emoji
-----------------------------------------
Load every emoji in a server (or in all servers the bot is in) into the bot's "hash map", which keeps track of the MD5 hashes of the images of all emoji it's encountered so far.

.. note::
    This happens automatically whenever a bot joins a new server, and the only purpose of running this command is to add new emoji it hasn't seen before from a server it was already in.

.. important::
    This command can only be run from the bot's special emoji server and only if you have Create Expressions and Manage Expressions permissions in it.

**Syntax:** ``/hash_server_emoji [server_id]``

- ``server_id``
    The numerical ID of a server whose emoji you want to load. Optional, if not included will hash every emoji from every server the bot is in.


/help
------------------------------
Display a list of all commands available to the user if ``command`` is not included, or a detailed explanation of an individual command if it is.

**Syntax:** ``/help [command]``

- ``command``
    Optional, the name of the command to get help about.


List Reactions
------------------------------
You can right-click a message the bot has access to and go to ``Apps > List Reactions`` for the bot to show you a list of all reactions on that message as well as who added each of them, including all reactions across bridges. This will work even for messages that aren't bridged, though.

.. note::
    Only reactions that the bot can "see" from the current channel—i.e., reactions to the message itself and reactions to bridged versions of the message across *inbound* bridges—will be listed.
