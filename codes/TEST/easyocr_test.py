#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Mar 19 11:04:53 2024

@author: nidhigoel
"""

import easyocr
import cv2

reader = easyocr.Reader(['en'],gpu=False)
result = reader.readtext(r"C:\Users\Administrator\AdQvestDir\captcha_gray.png")
print(result)
