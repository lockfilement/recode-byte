def remove_emojis_from_user(cog, user_id, emojis):
    if user_id in cog.user_emojis:
        if emojis:
            removed = []
            for emoji in emojis:
                if emoji in cog.user_emojis[user_id]:
                    cog.user_emojis[user_id].remove(emoji)
                    removed.append(emoji)
            if removed:
                return f"Removed emojis {', '.join(removed)} for user {user_id}"
            else:
                return f"No matching emojis found for user {user_id}"
        else:
            del cog.user_emojis[user_id]
            return f"Removed all emojis for user {user_id}"
    else:
        return f"No auto-react emoji set for user {user_id}"