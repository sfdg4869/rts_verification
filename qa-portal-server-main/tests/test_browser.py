#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
브라우저 창 자동 열기 테스트
"""

import sys
import time
import requests
import json

def test_browser_login():
    """브라우저 창을 띄워서 로그인 테스트"""
    
    # 테스트할 URL과 로그인 정보
    test_url = "http://10.20.132.40:18080/MAXGAUGE"
    username = "admin"
    password = "manager"
    
    # Flask 서버 URL
    flask_url = "http://localhost:5000/test/web_login"
    
    # 요청 데이터
    data = {
        "url": test_url,
        "username": username,
        "password": password,
        "show_browser": True,  # 브라우저 창 표시
        "keep_browser_open": True,  # 브라우저 창 유지
        "timeout": 30
    }
    
    print("=== 브라우저 창을 띄워서 로그인 테스트 시작 ===")
    print(f"테스트 URL: {test_url}")
    print(f"사용자명: {username}")
    print(f"브라우저 창 표시: {data['show_browser']}")
    print(f"브라우저 창 유지: {data['keep_browser_open']}")
    print()
    
    try:
        print("Flask 서버에 로그인 요청 전송 중...")
        response = requests.post(flask_url, json=data, timeout=60)
        
        print(f"응답 상태 코드: {response.status_code}")
        
        if response.status_code == 200:
            result = response.json()
            print("=== 로그인 테스트 결과 ===")
            print(f"성공 여부: {result.get('success', False)}")
            print(f"사용된 방법: {result.get('method_used', 'unknown')}")
            print(f"응답 시간: {result.get('response_time', 0)}초")
            print(f"메시지: {result.get('message', 'No message')}")
            print(f"응답 URL: {result.get('response_url', 'No URL')}")
            
            # Selenium 로그가 있다면 출력
            if 'selenium_logs' in result:
                print("\n=== Selenium 로그 ===")
                for log in result['selenium_logs']:
                    print(f"  {log}")
            
            if result.get('success'):
                print("\n✓✓ 로그인 성공!")
                if result.get('keep_browser_open'):
                    print("브라우저 창이 열린 상태로 유지되고 있습니다.")
                    print("수동으로 브라우저를 확인하고 닫아주세요.")
            else:
                print("\n✗✗ 로그인 실패")
                
        else:
            print(f"서버 오류: {response.status_code}")
            print(response.text)
            
    except requests.exceptions.ConnectionError:
        print("❌ Flask 서버에 연결할 수 없습니다.")
        print("먼저 'python app.py'로 서버를 실행해주세요.")
    except requests.exceptions.Timeout:
        print("❌ 요청 타임아웃")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

def test_simple_browser():
    """간단한 브라우저 창 테스트"""
    print("=== 간단한 브라우저 창 테스트 ===")
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        print("Chrome WebDriver 생성 중...")
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--no-sandbox')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # 헤드리스 모드 비활성화 (브라우저 창 표시)
        # chrome_options.add_argument('--headless')  # 이 줄을 주석처리
        
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
            print("✓ ChromeDriverManager를 사용하여 드라이버 생성 성공")
        except ImportError:
            driver = webdriver.Chrome(options=chrome_options)
            print("✓ 기본 ChromeDriver 사용")
        
        print("브라우저 창이 열렸습니다!")
        
        # 테스트 URL로 이동
        test_url = "http://10.20.132.40:18080/MAXGAUGE"
        print(f"URL 접속 시도: {test_url}")
        
        try:
            driver.get(test_url)
            print(f"✓ 페이지 로드 성공: {driver.title}")
            print(f"현재 URL: {driver.current_url}")
            
            # 5초 대기
            print("5초 후 브라우저를 닫습니다...")
            time.sleep(5)
            
        except Exception as e:
            print(f"페이지 로드 실패: {e}")
            # 그래도 3초 대기해서 브라우저 창을 확인할 수 있게 함
            print("3초 후 브라우저를 닫습니다...")
            time.sleep(3)
        
        driver.quit()
        print("브라우저가 닫혔습니다.")
        
    except ImportError:
        print("❌ Selenium이 설치되지 않았습니다.")
        print("pip install selenium webdriver-manager로 설치해주세요.")
    except Exception as e:
        print(f"❌ 브라우저 테스트 실패: {e}")

if __name__ == "__main__":
    print("어떤 테스트를 실행하시겠습니까?")
    print("1. 브라우저 창을 띄워서 로그인 테스트 (Flask 서버 필요)")
    print("2. 간단한 브라우저 창 테스트")
    print("3. 둘 다 실행")
    
    choice = input("선택 (1, 2, 3): ").strip()
    
    if choice == "1":
        test_browser_login()
    elif choice == "2":
        test_simple_browser()
    elif choice == "3":
        test_simple_browser()
        print("\n" + "="*50 + "\n")
        test_browser_login()
    else:
        print("잘못된 선택입니다. 간단한 브라우저 테스트를 실행합니다.")
        test_simple_browser()
