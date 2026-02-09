def toggle_auto_react(cog, user_identifiers, emojis=None):
    results = []
    for identifier in user_identifiers:
        if isinstance(identifier, int):
            # It's a user ID
            user_id = identifier
            if user_id in cog.user_auto_react and cog.user_auto_react[user_id]:
                cog.user_auto_react[user_id] = False
                if user_id in cog.user_emojis:
                    del cog.user_emojis[user_id]
                results.append((user_id, False, "disabled"))
            else:
                cog.user_auto_react[user_id] = True
                if emojis:
                    cog.user_emojis[user_id] = emojis
                results.append((user_id, True, "enabled"))
        else:
            # It's a username
            if identifier in cog.user_auto_react and cog.user_auto_react[identifier]:
                cog.user_auto_react[identifier] = False
                if identifier in cog.user_emojis:
                    del cog.user_emojis[identifier]
                results.append((identifier, False, "disabled"))
            else:
                cog.user_auto_react[identifier] = True
                if emojis:
                    cog.user_emojis[identifier] = emojis
                results.append((identifier, True, "enabled"))
    return results
