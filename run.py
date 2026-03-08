from kemono_library import create_app

app = create_app()


if __name__ == "__main__":
    app.run(
        debug=True,
        # Keep live-reload for source changes, but ignore runtime library data writes.
        exclude_patterns=[
            "data/*",
            "data/**",
            "*/data/*",
            "*/data/**",
            "data\\*",
            "data\\**",
            "*\\data\\*",
            "*\\data\\**",
        ],
    )
