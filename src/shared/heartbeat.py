"""Lambda for static HTML at API gateway root"""

import os


def heartbeat_handler(event, context):
    """A static response indicating the proxy is up"""
    del event
    del context
    display_name = os.environ.get("DISPLAY_NAME", "Heartbeat")
    doc_url = os.environ.get("DOC_URL", "https://docs.smarthealthit.org/cumulus/")

    response_body = f"""
    <!DOCTYPE html>
    <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <title>{display_name}</title>
            <style>
                body {{ font-family: sans-serif; margin: 2em; background: #f8f9fa; }}
                h1 {{ color: #2c3e50; }}
                a {{ margin: 1em 0; color: #007bff; text-decoration: none; }}
                a:hover {{ text-decoration: underline; }}
            </style>
        </head>
        <body>
            <h1>{display_name}</h1>
            <p> See the <a href ="{doc_url}">documentation site</a> for more info</p>
        </body>
    </html>
    """
    return {
        "statusCode": 200,
        "body": response_body,
        "headers": {
            "Content-Type": "text/html",
        },
    }
