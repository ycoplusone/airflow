# smtp 메일 발송 코드 테스트
import smtplib

# 1. SMTP 서버 연결 (TLS 기준 587 포트)
smtp = smtplib.SMTP('smtp.gmail.com', 587)

# 2. TLS 암호화 시작 (성공 시 220 대답이 옵니다)
smtp.starttls()

# 3. ⭐️ 로그인 테스트 (본인 계정과 앱 비밀번호 입력)
# 성공하면 (235, b'2.7.0 Accepted') 라는 메시지가 뜹니다!
aa = smtp.login('ycoplusone@gmail.com', 'fwcguirtiqbqdvfr')
print(aa)

# 4. 연결 안전하게 종료
smtp.quit()