#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
MAXGAUGE 로그인 및 시스템 상태 체크 테스트
"""

import requests
import json
import time

def test_maxgauge_login_with_status():
    """MAXGAUGE 로그인 후 시스템 상태 체크 테스트"""
    
    # 테스트 데이터
    test_data = {
        "url": "http://10.20.132.101:15080/MAXGAUGE",  # 실제 MAXGAUGE URL
        "username": "admin",  # 실제 사용자명
        "password": "manager",  # 실제 비밀번호
        "show_browser": True,  # 브라우저 창 표시
        "keep_browser_open": True,  # 브라우저 창 유지
        "timeout": 30
    }
    
    print("MAXGAUGE 로그인 및 시스템 상태 체크 테스트 시작...")
    print(f"URL: {test_data['url']}")
    print(f"사용자명: {test_data['username']}")
    print(f"브라우저 표시: {test_data['show_browser']}")
    print(f"브라우저 유지: {test_data['keep_browser_open']}")
    print("=" * 50)
    
    try:
        # Flask 애플리케이션 엔드포인트 호출
        response = requests.post(
            "http://localhost:5000/test/web_login",
            json=test_data,
            timeout=60  # 충분한 시간 제공
        )
        
        if response.status_code == 200:
            result = response.json()
            
            print("✅ 테스트 결과:")
            print(f"   로그인 성공: {result.get('success', False)}")
            print(f"   메시지: {result.get('message', '')}")
            print(f"   응답 시간: {result.get('response_time', 0)}초")
            
            # 시스템 상태 정보 출력
            system_status = result.get('system_status')
            if system_status:
                print("\n📊 시스템 상태:")
                print(f"   RTS: {'🟢 온라인' if system_status.get('rts_online', False) else '🔴 오프라인'}")
                print(f"   DB: {'🟢 온라인' if system_status.get('db_online', False) else '🔴 오프라인'}")
                print(f"   시스템 준비: {'✅ 완료' if system_status.get('system_ready', False) else '⚠️ 부분적'}")
                
                # 스크린샷 정보
                if 'screenshot' in system_status:
                    print(f"   📸 스크린샷: {system_status['screenshot']}")
                
                # 인스턴스 선택 정보
                if 'selected_instance' in system_status:
                    print(f"   🎯 선택된 인스턴스: {system_status['selected_instance']}")
                
                # 인스턴스 선택 후 스크린샷
                if 'screenshot_after_select' in system_status:
                    print(f"   📸 선택 후 스크린샷: {system_status['screenshot_after_select']}")
                
                system_message = result.get('system_message', '')
                if system_message:
                    print(f"   상태 메시지: {system_message}")
            else:
                print("\n⚠️ 시스템 상태 정보 없음")
            d
            # 스크린샷 경로 정보
            screenshot_path = result.get('screenshot_path')
            if screenshot_path:
                print(f"\n📸 전체 페이지 스크린샷: {screenshot_path}")
            
            # 인스턴스 선택 후 스크린샷
            screenshot_after_select = result.get('screenshot_after_select')
            if screenshot_after_select:
                print(f"📸 인스턴스 선택 후 스크린샷: {screenshot_after_select}")
            
            # 선택된 인스턴스 정보
            selected_instance = result.get('selected_instance')
            if selected_instance:
                print(f"🎯 선택된 인스턴스: {selected_instance}")
            
            # 디버깅 로그 출력 (간소화)
            selenium_logs = result.get('selenium_logs', [])
            if selenium_logs:
                print("\n📝 주요 로그:")
                for log in selenium_logs[-5:]:  # 마지막 5개만 출력
                    print(f"   {log}")
            
            if result.get('success', False):
                print("\n🎉 테스트 성공! 브라우저 창에서 결과를 확인하세요.")
                if system_status and system_status.get('system_ready', False):
                    print("✨ RTS와 DB가 모두 온라인 상태입니다!")
                else:
                    print("⚠️ 일부 시스템이 준비되지 않았습니다.")
            else:
                print("\n❌ 테스트 실패")
        else:
            print(f"❌ HTTP 오류: {response.status_code}")
            print(f"응답: {response.text}")
            
    except requests.exceptions.ConnectionError:
        print("❌ Flask 애플리케이션에 연결할 수 없습니다.")
        print("먼저 'python app.py'로 서버를 시작하세요.")
    except Exception as e:
        print(f"❌ 테스트 중 오류 발생: {e}")

if __name__ == "__main__":
    test_maxgauge_login_with_status()
