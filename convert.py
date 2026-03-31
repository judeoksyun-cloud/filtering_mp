import base64
from PIL import Image

img_path = r"C:\Users\YUN JUDEOK\.gemini\antigravity\brain\32bf88d3-2834-4be8-8c11-913ebd3817ee\data_filter_icon_1774795177731.png"
img = Image.open(img_path)
img.save("icon.ico", format="ICO", sizes=[(256, 256), (128, 128), (64, 64), (32, 32), (16, 16)])

with open(img_path, "rb") as f:
    b64_str = base64.b64encode(f.read()).decode("utf-8")

with open("icon_b64.txt", "w") as f:
    f.write(b64_str)
