#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Chrome 브라우저 창 테스트 스크립트
"""

import time
import sys
import os

# 현재 디렉토리를 Python path에 추가
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.routes.web_login_test import create_selenium_driver

def test_chrome_browser():
    """Chrome 브라우저 창이 제대로 나타나는지 테스트"""
    print("Chrome 브라우저 창 테스트 시작...")
    
    # 브라우저 창을 표시하도록 설정 (headless=False)
    driver = create_selenium_driver(headless=False)
    
    if driver:
        try:
            print("Chrome 브라우저가 성공적으로 시작되었습니다!")
            print("테스트 URL로 이동 중...")
            
            # 간단한 테스트 페이지로 이동
            driver.get("https://www.google.com")
            
            print("Google 페이지에 접속했습니다.")
            print("브라우저 창을 확인해보세요!")
            
            # 10초 동안 브라우저 창 유지
            print("10초 후 브라우저가 자동으로 닫힙니다...")
            time.sleep(10)
            
        except Exception as e:
            print(f"테스트 중 오류 발생: {e}")
        finally:
            print("브라우저를 닫는 중...")
            driver.quit()
            print("테스트 완료!")
    else:
        print("Chrome 브라우저 생성에 실패했습니다.")

if __name__ == "__main__":
    test_chrome_browser()
