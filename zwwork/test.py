import re
import json


a = '[{"url": "https://wx6dd439ffc27afa14.zhangwenwenhua.com/index/book/chapter?book_id=10042564", "cost": "", "push": "1", "type": "0", "title": "📚《你是澎湃的海》--她瞧不起他，诋毁他，讨厌他。可有一天，谁都没料到，这人竟然一跃成为她的丈夫。", "book_id": "10042564", "wx_type": "1", "book_name": "你是澎湃的海", "title_type": "0", "channel_name": "0427客服", "guide_chapter_idx": "8"}]'


b = json.loads(a)

print(b[0])

