# 添加自定义样式
def daily_email_style(html_table):
    return """
    <html>
    <head>
    <style>
        .table {
            width: 100%;
            margin-bottom: 1rem;
            color: #212529;
        }
        .table-striped tbody tr:nth-of-type(odd) {
            background-color: rgba(0, 0, 0, 0.05);
        }
        .table-hover tbody tr:hover {
            color: #212529;
            background-color: rgba(0, 0, 0, 0.075);
        }
        th {
            vertical-align: bottom;
            border-bottom: 2px solid #dee2e6;
        }
        td, th {
            padding: .75rem;
            vertical-align: top;
            border-top: 1px solid #dee2e6;
        }
    </style>
    </head>
    <body>
    """ + html_table + """
    </body>
    </html>
    """
