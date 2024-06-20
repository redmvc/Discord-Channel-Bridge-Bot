import discord


class Bridges:
    """Each bridge is identified by a source channel/thread ID and a dict where each entry's key is a target channel/thread ID and its value is a webhook in the targets."""

    def __init__(self, source_id: int) -> None:
        self.source_id = source_id
        self.targets: dict[int, discord.Webhook] = {}

    def get_webhooks(self) -> dict[int, discord.Webhook]:
        return self.targets

    def get_webhook(
        self, target: discord.TextChannel | discord.Thread | int
    ) -> tuple[int, discord.Webhook | None]:
        if not isinstance(target, int):
            target = target.id

        return (target, self.targets.get(target))

    async def add_target(
        self,
        target: discord.TextChannel | discord.Thread,
        new_webhook: discord.Webhook | None = None,
    ) -> None:
        target_id = target.id

        await self.demolish(target_id)

        if new_webhook:
            self.targets[target_id] = new_webhook
        else:
            if isinstance(target, discord.Thread):
                webhook_channel = target.parent
            else:
                webhook_channel = target
            assert isinstance(webhook_channel, discord.TextChannel)
            self.targets[target_id] = await webhook_channel.create_webhook(
                name=f":bridge: ({self.source_id} {target_id})"
            )

    async def demolish(
        self, target: discord.TextChannel | discord.Thread | int
    ) -> None:
        target, webhook = self.get_webhook(target)
        if webhook:
            await webhook.delete(reason="User request.")
            del self.targets[target]
