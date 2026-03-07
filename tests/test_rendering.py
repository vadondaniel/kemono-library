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
