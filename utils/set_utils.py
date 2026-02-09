def add_emojis_to_user(cog, user_id, emojis):
    if user_id not in cog.user_emojis:
        cog.user_emojis[user_id] = []
    cog.user_emojis[user_id].extend(emojis)
    return f"Emojis {', '.join(emojis)} added for user {user_id}"