from common.ParserName import ParserName
from common.ActionName import ActionName
from lambda_function import lambda_handler

e = {
    "parser": ParserName.metro.name,
    "action": ActionName.MAKE_CSV.value,
    "use_proxy": False,
    "page_size": 500,
    "offset": 0,
    "has_more": True,
}

print(lambda_handler(e, None))