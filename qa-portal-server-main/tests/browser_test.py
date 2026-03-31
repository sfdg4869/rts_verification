#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
간단한 브라우저 창 테스트
"""

import requests
import json
import time

def test_browser_window():
    """브라우저 창 테스트"""
    
    # Flask 서버가 실행 중인지 확인
    try:
        response = requests.get("http://localhost:5000/", timeout=5)
        print("✓ Flask 서버가 실행 중입니다.")
    except:
        print("❌ Flask 서버가 실행되지 않았습니다. 먼저 'python app.py'로 서버를 실행해주세요.")
        return
    
    # 테스트 데이터
    test_data = {
        "url": "http://10.20.132.40:18080/MAXGAUGE",
        "username": "admin", 
        "password": "manager",
        "show_browser": True,      # 브라우저 창 표시
        "keep_browser_open": True, # 브라우저 창 유지
        "timeout": 30
    }
    
    print("=== 브라우저 창 테스트 시작 ===")
    print(f"URL: {test_data['url']}")
    print(f"브라우저 창 표시: {test_data['show_browser']}")
    print(f"브라우저 창 유지: {test_data['keep_browser_open']}")
    print()
    
    print("로그인 요청을 보내는 중...")
    print("잠시 후 브라우저 창이 나타날 예정입니다!")
    print()
    
    try:
        response = requests.post(
            "http://localhost:5000/test/web_login",
            json=test_data,
            timeout=60
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print("=== 테스트 결과 ===")
            print(f"성공: {result.get('success', False)}")
            print(f"메서드: {result.get('method_used', 'unknown')}")
            print(f"메시지: {result.get('message', 'No message')}")
            print(f"응답 시간: {result.get('response_time', 0)}초")
            
            if 'selenium_logs' in result:
                print("\n=== Selenium 로그 ===")
                for log in result['selenium_logs']:
                    print(f"  {log}")
            
            if result.get('success'):
                print("\n✅ 로그인 성공!")
            else:
                print("\n❌ 로그인 실패")
                
            if test_data['keep_browser_open'] and test_data['show_browser']:
                print("\n🌐 브라우저 창이 열린 상태로 유지되어야 합니다.")
                print("💡 브라우저 창에서 로그인 결과를 확인해보세요!")
                
        else:
            print(f"❌ 서버 오류: {response.status_code}")
            print(response.text)
            
    except requests.exceptions.Timeout:
        print("❌ 요청 타임아웃")
    except Exception as e:
        print(f"❌ 오류 발생: {e}")

if __name__ == "__main__":
    test_browser_window()
