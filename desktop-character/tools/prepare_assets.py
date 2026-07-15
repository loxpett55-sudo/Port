#!/usr/bin/env python3
"""Готовит слои-ассеты приложения из двух исходных изображений персонажа.

Вход:
  --closed  путь к изображению с закрытыми глазами (базовое состояние)
  --open    путь к изображению с открытыми глазами

Выход (в ../assets):
  base-closed.jpg   — базовый слой (весь кадр, глаза закрыты)
  eye-left.png      — патч левого глаза (открыт) с растушёванной альфой
  eye-right.png     — патч правого глаза (открыт) с растушёванной альфой
  eyes.json         — координаты патчей в системе координат сцены 1024x1024
  tray.png          — иконка трея 32x32
  ../build/icon.png — иконка приложения 512x512

Патчи вырезаются из "открытого" кадра по областям, где кадры реально
различаются (найдено попиксельным diff'ом), с запасом по краям и мягкой
альфа-растушёвкой — так патч бесшовно ложится поверх базового слоя и его
можно сдвигать на несколько пикселей для эффекта слежения за курсором.
"""
import argparse
import json
import os

from PIL import Image, ImageDraw, ImageFilter

HERE = os.path.dirname(os.path.abspath(__file__))
ASSETS = os.path.join(HERE, "..", "assets")
BUILD = os.path.join(HERE, "..", "build")

# Области различий (глаза) в координатах 1024x1024, найденные diff'ом кадров.
EYES = {
    "eye-left": {"box": (330, 658, 438, 757)},
    "eye-right": {"box": (599, 611, 720, 712)},
}
MARGIN = 30   # запас вокруг области различий, px
FEATHER = 22  # ширина растушёвки альфы, px


def feathered_crop(img: Image.Image, box, margin, feather):
    x0, y0, x1, y1 = box
    x0, y0 = max(0, x0 - margin), max(0, y0 - margin)
    x1, y1 = min(img.width, x1 + margin), min(img.height, y1 + margin)
    patch = img.crop((x0, y0, x1, y1)).convert("RGBA")

    mask = Image.new("L", patch.size, 0)
    d = ImageDraw.Draw(mask)
    d.rounded_rectangle(
        (feather, feather, patch.width - feather, patch.height - feather),
        radius=feather, fill=255,
    )
    mask = mask.filter(ImageFilter.GaussianBlur(feather / 2))
    patch.putalpha(mask)
    return patch, (x0, y0, x1, y1)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--closed", required=True)
    ap.add_argument("--open", required=True)
    args = ap.parse_args()

    os.makedirs(ASSETS, exist_ok=True)
    os.makedirs(BUILD, exist_ok=True)

    closed = Image.open(args.closed).convert("RGB")
    opened = Image.open(args.open).convert("RGB")
    if opened.size != closed.size:
        opened = opened.resize(closed.size, Image.LANCZOS)

    closed.save(os.path.join(ASSETS, "base-closed.jpg"), quality=92)

    meta = {"scene": {"width": closed.width, "height": closed.height}, "eyes": {}}
    for name, cfg in EYES.items():
        patch, placed = feathered_crop(opened, cfg["box"], MARGIN, FEATHER)
        patch.save(os.path.join(ASSETS, f"{name}.png"))
        meta["eyes"][name] = {
            "x": placed[0], "y": placed[1],
            "w": placed[2] - placed[0], "h": placed[3] - placed[1],
        }
    with open(os.path.join(ASSETS, "eyes.json"), "w") as f:
        json.dump(meta, f, indent=2)

    # Иконки: лицо персонажа крупным планом.
    face = opened.crop((250, 380, 850, 980))
    face.resize((512, 512), Image.LANCZOS).save(os.path.join(BUILD, "icon.png"))
    face.resize((32, 32), Image.LANCZOS).save(os.path.join(ASSETS, "tray.png"))
    print("assets written:", json.dumps(meta, indent=2))


if __name__ == "__main__":
    main()
