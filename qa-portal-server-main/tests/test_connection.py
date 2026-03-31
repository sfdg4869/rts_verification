#!/usr/bin/env python3
"""
연결 테스트 스크립트
MAXGAUGE 서버에 직접 연결을 시도하고 브라우저로도 확인해봅니다.
"""

import requests
import time
from urllib.parse import urlparse

def test_connection(url, timeout=10):
    """기본 연결 테스트"""
    print(f"=== 연결 테스트: {url} ===")
    
    try:
        # URL 정규화
        if not url.startswith(('http://', 'https://')):
            url = 'http://' + url
        
        parsed = urlparse(url)
        print(f"파싱된 URL: 스키마={parsed.scheme}, 호스트={parsed.hostname}, 포트={parsed.port}")
        
        # 연결 시도
        print(f"연결 시도 중... (타임아웃: {timeout}초)")
        start_time = time.time()
        
        response = requests.get(url, timeout=timeout)
        elapsed_time = time.time() - start_time
        
        print(f"✓ 연결 성공!")
        print(f"  - 상태 코드: {response.status_code}")
        print(f"  - 응답 시간: {elapsed_time:.2f}초")
        print(f"  - 응답 URL: {response.url}")
        print(f"  - Content-Type: {response.headers.get('Content-Type', 'N/A')}")
        print(f"  - 응답 크기: {len(response.content)} bytes")
        
        # HTML 내용 미리보기
        if response.headers.get('Content-Type', '').startswith('text/html'):
            html_preview = response.text[:500]
            print(f"  - HTML 미리보기 (처음 500자):")
            print(f"    {html_preview}")
        
        return True, response
        
    except requests.exceptions.ConnectionError as e:
        print(f"✗ 연결 실패: {e}")
        print(f"  - 서버가 실행 중인지 확인하세요")
        print(f"  - 방화벽 설정을 확인하세요")
        print(f"  - URL과 포트가 올바른지 확인하세요")
        return False, None
        
    except requests.exceptions.Timeout:
        print(f"✗ 타임아웃: {timeout}초 내에 응답이 없습니다")
        print(f"  - 서버가 응답하지 않거나 네트워크가 느릴 수 있습니다")
        return False, None
        
    except Exception as e:
        print(f"✗ 예상치 못한 오류: {e}")
        return False, None

def test_with_selenium(url):
    """Selenium으로 브라우저 창 열어서 테스트"""
    print(f"\n=== Selenium 브라우저 테스트: {url} ===")
    
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        
        # Chrome 옵션 설정 (브라우저 창 표시)
        chrome_options = Options()
        chrome_options.add_argument('--window-size=1920,1080')
        chrome_options.add_argument('--start-maximized')
        chrome_options.add_argument('--disable-dev-shm-usage')
        
        # ChromeDriver 생성
        try:
            from webdriver_manager.chrome import ChromeDriverManager
            driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except ImportError:
            driver = webdriver.Chrome(options=chrome_options)
        
        print("브라우저 창이 열립니다...")
        driver.get(url)
        
        print(f"✓ 브라우저로 페이지 로드 성공!")
        print(f"  - 현재 URL: {driver.current_url}")
        print(f"  - 페이지 제목: {driver.title}")
        
        # 5초 기다린 후 사용자에게 확인 요청
        print("\n브라우저 창에서 페이지를 확인하세요.")
        print("확인 후 아무 키나 누르면 브라우저가 닫힙니다...")
        input()
        
        driver.quit()
        print("브라우저가 닫혔습니다.")
        return True
        
    except ImportError:
        print("✗ Selenium이 설치되어 있지 않습니다.")
        print("  pip install selenium webdriver-manager 명령으로 설치하세요.")
        return False
        
    except Exception as e:
        print(f"✗ Selenium 테스트 실패: {e}")
        return False

if __name__ == "__main__":
    # 테스트할 URL
    test_url = "http://10.20.132.40:18080/MAXGAUGE"
    
    print("MAXGAUGE 서버 연결 테스트를 시작합니다...\n")
    
    # 1단계: 기본 연결 테스트
    success, response = test_connection(test_url, timeout=30)
    
    if success:
        print(f"\n🎉 기본 연결 테스트 성공!")
        
        # 2단계: Selenium 브라우저 테스트
        print(f"\n브라우저 창으로도 확인해보시겠습니까? (y/n): ", end="")
        choice = input().lower()
        
        if choice == 'y':
            test_with_selenium(test_url)
    else:
        print(f"\n❌ 기본 연결 테스트 실패")
        print(f"서버 상태를 확인하고 다시 시도해주세요.")
    
    print(f"\n테스트가 완료되었습니다.")
