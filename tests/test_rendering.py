from kemono_library.rendering import render_post_content


def test_render_rewrites_full_and_short_links():
    content = (
        '<p>One <a href="https://kemono.cr/fanbox/user/11/post/22">full</a> '
        'and <a href="https://kemono.cr/fanbox/post/33">short</a>.</p>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="11",
        current_post_id=77,
    )
    assert "/links/resolve?service=fanbox&amp;post=22&amp;from_post=77&amp;user=11" in rendered
    assert (
        "/links/resolve?service=fanbox&amp;post=33&amp;from_post=77&amp;user=11&amp;assumed_from_context=1"
        in rendered
    )


def test_render_turns_empty_image_anchor_into_img():
    content = (
        '<a href="https://downloads.fanbox.cc/images/post/10791194/'
        'eO8wzPLjankw59mg6YeTMzxN.jpeg" rel="noopener noreferrer"></a>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
    )
    assert "<a " in rendered
    assert "<img" in rendered
    assert 'target="_blank"' in rendered
    assert 'src="https://downloads.fanbox.cc/images/post/10791194/eO8wzPLjankw59mg6YeTMzxN.jpeg"' in rendered


def test_render_rewrites_inline_media_to_local_url_by_name():
    content = (
        '<a href="https://downloads.fanbox.cc/images/post/10791194/eO8wzPLjankw59mg6YeTMzxN.jpeg" '
        'rel="noopener noreferrer"></a>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
        local_media_map={
            "https://kemono.cr/72/3a/723a7e8e7172e528627eb060371fb29a4afdc75e7fc97e78eafbe97196f9c2cc.jpg": "/files/post_1/eO8wzPLjankw59mg6YeTMzxN.jpeg"
        },
        local_media_by_name={"eo8wzpljankw59mg6yetmzxn.jpeg": "/files/post_1/eO8wzPLjankw59mg6YeTMzxN.jpeg"},
    )
    assert 'href="/files/post_1/eO8wzPLjankw59mg6YeTMzxN.jpeg"' in rendered
    assert 'src="/files/post_1/eO8wzPLjankw59mg6YeTMzxN.jpeg"' in rendered


def test_render_rewrites_file_link_using_anchor_text_alias():
    content = (
        '<p><a href="https://downloads.fanbox.cc/files/post/10791194/H9U6jEFTAYx8c4nanzHWqQWv.zip" '
        'rel="noopener noreferrer">Break Room</a></p>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
        local_media_map={
            "https://kemono.cr/be/0d/be0d1fe75e8e5786732a20e5a0ac9f013e0848a86cb7f7b752c887f4c9ea06ad.zip": "/files/post_1/Break_Room.zip"
        },
        local_media_by_name={"break room.zip": "/files/post_1/Break_Room.zip"},
    )
    assert 'href="/files/post_1/Break_Room.zip"' in rendered
    assert ">Break Room<" in rendered


def test_render_rewrites_file_link_alias_with_numeric_label_suffix():
    content = (
        '<p><a href="https://downloads.fanbox.cc/files/post/11441751/EZNQnKOMdH7j94svQWqmIaPo.zip" '
        'rel="noopener noreferrer">Artwork No.34</a></p>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
        local_media_map={},
        local_media_by_name={"artwork no.34.zip": "/files/post_1/Artwork_No.34.zip"},
    )
    assert 'href="/files/post_1/Artwork_No.34.zip"' in rendered
    assert ">Artwork No.34<" in rendered


def test_render_falls_back_to_attachment_remote_url_when_local_missing():
    content = (
        '<a href="https://downloads.fanbox.cc/images/post/10791194/'
        'eO8wzPLjankw59mg6YeTMzxN.jpeg" rel="noopener noreferrer"></a>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
        local_media_map={},
        local_media_by_name={},
        remote_media_by_name={
            "eo8wzpljankw59mg6yetmzxn.jpeg": "https://n2.kemono.cr/c2/4f/hash.jpg",
        },
    )
    assert 'href="https://n2.kemono.cr/c2/4f/hash.jpg"' in rendered
    assert 'src="https://n2.kemono.cr/c2/4f/hash.jpg"' in rendered


def test_render_linkifies_and_rewrites_plain_fanbox_post_url():
    content = "<p>前のhttps://tetetoroort.fanbox.cc/posts/8644398</p>"
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
    )
    assert ">前の<a " in rendered
    assert (
        '/links/resolve?service=fanbox&amp;post=8644398&amp;from_post=1&amp;user=70479526&amp;assumed_from_context=1'
        in rendered
    )


def test_render_rewrites_linkified_fanbox_url_with_trailing_cjk_text():
    content = (
        "<p>pixiv\u30ea\u30af\u30a8\u30b9\u30c8\u3042\u308a\u304c\u3068\u3046\u3054\u3056\u3044\u307e\u3059\u3002"
        "https://tetetoroort.fanbox.cc/posts/6663853\u306e\u3064\u3065\u304d\u3002\u30ad\u30e3\u30e9\u30af\u30bf\u30fcAI"
        "\u306f\u30aa\u30d5\u306b\u306a\u3063\u3066\u3044\u308b\u306e\u3067\u3001\u5f7c\u5973\u305f\u3061\u306f\u7d20\u9762\u3067"
        "\u30ed\u30fc\u30eb\u30d7\u30ec\u30a4\u3092\u697d\u3057\u3093\u3067\u3044\u308b\u3088\u3046\u3067\u3059\u3002</p>"
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="67922",
        current_post_id=1,
    )
    assert (
        '/links/resolve?service=fanbox&amp;post=6663853&amp;from_post=1&amp;user=67922&amp;assumed_from_context=1'
        in rendered
    )
    assert "posts/6663853</a>\u306e\u3064\u3065\u304d" in rendered


def test_render_rewrites_www_fanbox_creator_post_link():
    content = (
        '<h3><a href="https://www.fanbox.cc/@tetetoroort/posts/9187463" '
        'rel="noopener noreferrer nofollow">【+3カット】りなちゃんゆめちゃんの休暇</a></h3>'
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
    )
    assert (
        '/links/resolve?service=fanbox&amp;post=9187463&amp;from_post=1&amp;user=70479526&amp;assumed_from_context=1'
        in rendered
    )


def test_render_groups_fanbox_promo_insert_block():
    content = (
        '<h3><a href="https://www.fanbox.cc/@tetetoroort/posts/10230546" '
        'rel="noopener noreferrer">【+3カット】猫のお嫁さん</a></h3>'
        '<p><img src="https://pixiv.pximg.net/c/1200x630_90_a2_g5/fanbox/public/images/post/10230546/cover/KfCD8SrNbG42RVD2idgCGqm2.jpeg"></p>'
        "<p>あともう少しで人型に戻る期日だったのに、ヤバそうなガールフレンドに見つかってしまうゆめちゃん。</p>"
    )
    rendered = render_post_content(
        content,
        current_service="fanbox",
        current_user_id="70479526",
        current_post_id=1,
    )
    assert '<section class="post-promo-insert">' in rendered
    assert (
        '/links/resolve?service=fanbox&amp;post=10230546&amp;from_post=1&amp;user=70479526&amp;assumed_from_context=1'
        in rendered
    )
