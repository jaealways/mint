import pandas as pd
import numpy as np


list_artist_mc = ['뮤직카우', '앤씨아', '견우', '이우', 'JBJ', '헤일로', 'NATURE', '이예준', '야다', 'AOA', '빅마마', '미교', '정동하',
                  '백지영', '민들레', '플라워', '윤상', '자이언트핑크', '에디킴', '멜로망스', '혜령', '양수경', '용주', '청하',
                  '주원', 'TWICE', '주영훈', '보이비', '소찬휘', '전유나', '브라운아이드걸스', '송가인', '카라', '빈지노', '루나솔라',
                  '울랄라세션', '엠블랙', '엄정화', '하성운', 'JBJ95', '임한별', '신화', '반하나', '치타', '원투', '편승엽', '박유천',
                  '우주소녀', '베스티', 'IKON', '태용', '이하이', '에릭남', 'Ecobridge', '민아', 'ASTRO', '아이오아이', 'Dok2',
                  '박장근', '유엔', '이달의소녀', '미스에스', '양파', '더 씨야', '데이브레이크', '알리', 'UNI.T', 'CLC', '스토니 스컹크',
                  '쎄쎄쎄', '멜로디데이', 'H&D', '윤하', '쿨', '에어플레인', '루나', '정엽', 'KUSH', '팀', '정미애', '강민혁', '박현빈',
                  '백아연', '몬스타엑스', '씨스타', '비투비-블루', '이수', '장혜리', '손승연', '최백호', '패티김', '사무엘', '박효신',
                  '바이브', '이준호', '콜딘', '정은지', '조PD', '맥켈리', '신신애', '왁스', '타이거JK', 'SUPER JUNIOR', '다이나믹 듀오',
                  '걸', '슈가볼', '김나영', 'SE7EN', '애프터스쿨', '이우상', '김범수', '더 크로스', '휘성', '적재', '서인영', 'MC THE MAX',
                  '오유진', 'DJ DOC', '홍진영', '손담비', '대성', '먼데이 키즈', '윤딴딴', '선우정아', '비비', '별', 'ILLIONAIRE RECORDS',
                  '임창정', '아이비', '성유진', '김장훈', '인피니트', '한동근', '다비치', '심신', '리사', '박원', '현아', '효린', 'G-DRAGON',
                  '진해성', 'NS 윤지', '여자친구', 'Zion.T', '구창모', '마이티 마우스', '레디', 'IZ*ONE', '유지', '태진아', '2NE1',
                  '아이유', '규현', '훈스', '기리보이', '화요비', '이수영', '원 모어 찬스', '소방차', '이승철', '김재중', '칵스',
                  'GG', '소향', '일락', '천상지희 더 그레이스', '이재훈', '벤', '지천비화', 'AKMU', '투개월', '전효성', '김태우',
                  '10CM', '뉴이스트', '김필', 'C JAMM', '고유진', '박혜경', 'BIGBANG', 'UV', '변진섭', '리쌍', '양혜승',
                  '유재석 X Dok2', '렉시', '티아라', 'EXO', 'SHINee', '백예린', '모모랜드', 'Various Artists', 'Various Artists',
                  'Various Artists', '선미', '박지훈',
                  'GD&TOP', 'SG 워너비', '에이프릴', '박봄', '정기고', '코요태', 'B1A4', '지아', '정인', '바비 킴', '김현성', '제로',
                  'F-ve Dolls', '수지', 'Steady', '제국의아이들', '기호', '오투포', '강수지', '매드클라운', '비투비', '송하예',
                  '거미', '한해', '하동균', 'BOBBY', '태양', '수호', '전우성', '케이윌', '제리케이', '송지은', '루그', 'Crush',
                  '김나희', 'Wanna One', '크레용팝', '버즈', '조성모', 'OurR', '이진혁', '라디', '컨츄리 꼬꼬', '마마무', '팔로알토',
                  '김보경', 'iKON', '황보', '주석', '양요섭', '애즈원', 'MC몽', '이병헌', '길구봉구', '소유', '시크릿', '메이비',
                  '지연', '일기예보', '딸기우유', '환희', '김수희', '스윙스', '드림캐쳐', '투아이즈', 'V.O.S', '박명수', '마리오',
                  '정준일', '임재범', '제시', '어쿠루브', 'god', '쏜애플', '장우혁', '스페이스 에이', '유열', 'MOBB', '도겸',
                  '알렉스', '자두', 'Lim Kim', '샘김', 'VIXX', '강다니엘', '이승기', '김형중', '김원준', '윙크', '가인', '박규리',
                  '조 트리오', '윤지성', '버스커 버스커', 'Apink', '박지윤', '2PM', '박보람', '비스트', '유미', '에일리', '테이',
                  '노리플라이', '이선희', '핑클', '브라운 아이드 소울', '다니엘', 'SURL', '수퍼비', 'izi', '지나', '김종민', '4minute',
                  '10cm', '바다', '소란', '범키', '씨야', '로꼬', 'ELSIE', '틴탑', '젝스키스', '유키스', '허각', '정세운', '홍대광',
                  '구피', 'The One', '샵', '홍자', '김현철', '지코', 'EXID', '김민우', '황치열', 'B.A.P', '헨리', '엠투엠',
                  '키썸', '걸스데이', 'GOT7', '이태종', '브레이브걸스', '이수현', '달샤벳', '린', '김재환', '벅', '원티드', '이루',
                  '헬로비너스', '터보', '엠씨더맥스', '일레인', '펀치', '조관우', '씨스타19', '찬열', '세정', '레게 강 같은 평화',
                  '김종국', '아웃렛', '현빈', '권순관', '정다경', '유주', '장나라', 'SUPER JUNIOR-K.R.Y.', 'PERC%NT', '화사',
                  '윤미래', 'XIA', '솔로몬', '백현', '라붐', '버블 시스터즈', 'DIA',

                    # 814곡 이후 추가분
                    '김세정', '송승헌', '비쥬', '이송연', '아이', '배치기', '차태현', '경서', '임도혁', '민현', '오이스터', 'Red Velvet',
                  '신승태', '산들', '양다일', '김부용', '오현란', '차은우', '이예린', 'GRAY', 'KCM', '조이', '강혜인', '위수',
                  '신촌블루스', '이정현', 'DEAN', '신미래', '이기찬', '솔지', '김현식', 'Ashily', '정유지', '로이킴', '지진석',
                  '강현주', '경상 아가씨', '이민혁', '조장혁', '오왠', '하현우', '이무진', '유승우', '장덕철', '한스밴드', '데이먼스 이어',
                  '소유미', '노을', '전철민', '셔누', '김경호', '하울', '소울스타', '서연', '지선', '진민호', '정효빈', '더 자두',
                  '썸데이', '재하', '죠지', '유니', '셀린', '박다혜', '어반자카파', '태무', '송윤아', '최향', '도도', '손성훈',
                  'SS501', 'Colde', '유노윤호', '태사비애', 'Luv', '전상근', '카이', '김민석', '티맥스', '밤에', 'SOUND PALETTE',
                  '러니', '조규만', '윤종신', '신예영', '박강성', '에이 스타일', '일렉트로보이즈', '첸', '최성수', '원더나인', '피플 크루',
                  '웬디', '이소정', 'LambC', '조현아', '신용재', '이기광', '채정안', '이동은', '승희', '하하', '빅스타', '김정민', '러블리즈',
                  '송민경', '지코', '가시찔레', '권인하', '김한결', '노라조', '반광옥', '보쌈', '유영석', '티아라 N4', '황인욱', '신미래',
                  '김현수']


# 솔로몬, 기호 : query nan,
# 쿼리 날릴 때는 뮤직카우 추가할 것
# various artist 3개:  '광해 왕이 된 남자',  '신민아 영화 키친',  '공유 영화 용의자',

list_artist_query = ['뮤직카우', '앤씨아', '견우 이지훈', '가수 이우', 'JBJ', '아이돌 헤일로', '아이돌 네이처', '이예준', '야다', 'AOA', '빅마마',
                     '미교', '정동하', '백지영', '민들레 신동국', '플라워 고유진', '윤상', '자이언트핑크', '에디킴', '멜로망스',
                     '가수 혜령', '양수경', '가수 용주', '청하', '주원', '트와이스', '주영훈', '보이비', '소찬휘', '전유나', '브라운아이드걸스',
                     '송가인', '아이돌 카라', '빈지노', '루나솔라', '울랄라세션', '엠블랙', '엄정화', '하성운', 'JBJ95', '임한별',
                     '아이돌 신화', '반하나', '치타', '가수 원투', '편승엽', '박유천', '우주소녀', '베스티', 'IKON', '태용', '이하이',
                     '에릭남', '가수 에코브릿지', '걸스데이 민아', '아스트로', '아이오아이', '래퍼 도끼', '가수 박장근', '유엔 김정훈',
                     '이달의소녀', '미스에스', '가수 양파', '더 씨야', '밴드 데이브레이크', '가수 알리', '아이돌 유니티', 'CLC',
                     '스토니 스컹크', '가수 쎄쎄쎄', '가수 멜로디데이', '가수 h&d', '윤하', '가수 쿨', '가수 에어플레인', '가수 루나',
                     '정엽', '가수 쿠시', '팀 황영민', '정미애', '강민혁', '박현빈', '백아연', '몬스타엑스', '씨스타', '비투비',
                     '가수 이수', '가수 장혜리', '손승연', '최백호', '패티김', '김사무엘', '박효신', '가수 바이브', '이준호 퇴근버스',
                     '콜딘', '정은지', '조PD', '맥켈리', '신신애', '가수 왁스', '타이거JK', '슈퍼주니어', '다이나믹 듀오', '가수 김세헌',
                     '가수 슈가볼', '김나영 그럴걸', '세븐', '애프터스쿨', '가수 이우상', '가수 김범수', '더 크로스', '휘성', '가수 적재',
                     '서인영', '엠씨더맥스', '가수 오유진', 'DJ DOC', '홍진영', '손담비', '빅뱅 대성', '먼데이 키즈', '윤딴딴', '선우정아',
                     '비비', '가수 별', '일리네어 레코즈', '임창정', '가수 아이비', '씨야 성유진', '김장훈',
                     '아이돌 인피니트', '한동근', '다비치', '90년대 가수 심신', '발라드 리사', '가수 박원', '현아', '효린', '지드래곤',
                     '진해성', 'NS윤지', '아이돌 여자친구', '자이언티', '가수 구창모', '마이티마우스', '김홍우 레디', '아이즈원', '정유지',
                     '태진아', '2NE1', '아이유', '규현', '가수 훈스', '기리보이', '화요비', '가수 이수영', '가수 원모어찬스', '가수 소방차',
                     '이승철', '김재중', '가수 칵스', '박명수 지드래곤 GG', '소향', '가수 일락', '천상지희 더 그레이스', '쿨 이재훈',
                     '가수 벤', '지천비화', '악동뮤지션', '투개월', '전효성', '가수 김태우', '가수 10CM', '뉴이스트', '김필', '씨잼',
                     '가수 고유진', '가수 박혜경', '빅뱅', '유세윤 UV', '변진섭', '리쌍', '양혜승', '유재석 X Dok2', '가수 렉시',
                     '티아라', '엑소', '샤이니', '백예린', '모모랜드', '광해 왕이 된 남자', '신민아 영화 키친', '공유 영화 용의자',
                     '선미', '가수 박지훈', 'GD&TOP', 'SG워너비', '아이돌 에이프릴', '박봄', '정기고', '코요태', 'B1A4', '발라드 지아',
                     '가수 정인', '바비킴', '가수 김현성', '가수 제로 박성철', '파이브돌스', '가수 수지', '스테디 이현경', '제국의아이들',
                     np.nan, '오투포', '가수 강수지', '매드클라운', '비투비', '송하예', '가수 거미', '정한해', '하동균', '아이콘 바비', '빅뱅 태양',
                     '엑소 수호', '노을 전우성', '케이윌', '제리케이', '시크릿 송지은', '루그', '래퍼 크러쉬', '김나희', '워너원',
                     '크레용팝', '발라드 버즈', '조성모', '아월', '업텐션 이진혁', '가수 라디', '컨츄리꼬꼬', '마마무', '래퍼 팔로알토',
                     '가수 김보경', 'yg 아이콘', '가수 황보', '래퍼 박주석', '양요섭', '애즈원', 'MC몽', '이병헌', '길구봉구', '씨스타 소유',
                     '가수 시크릿', '메이비', '티아라 지연', '가수 일기예보', '초아 딸기우유', '가수 환희', '가수 김수희', '스윙스',
                     '드림캐쳐', '가수 투아이즈', 'V.O.S', '박명수', '마리오 정한림', '정준일', '임재범', '가수 제시 jessi', '어쿠루브',
                     '가수 god', '쏜애플', 'HOT 장우혁', '스페이스 에이', '가수 유열', '송민호 MOBB', '도겸', '드리핀 알렉스', '가수 자두',
                     '김예림 Lim Kim', '샘김', '가수 VIXX', '강다니엘', '이승기', '가수 김형중', '가수 김원준', '트로트 윙크', '브아걸 가인',
                     '박규리', '조 트리오', '윤지성', '그룹 버스커 버스커', '에이핑크', '가수 박지윤', '2PM', '가수 박보람', '아이돌 비스트',
                     '가수 유미 오유미', '에일리', '가수 테이', '노리플라이', '이선희', '핑클', '브라운 아이드 소울', '달마시안 다니엘',
                     'SURL', '수퍼비', 'izi', '가수 지나 G.NA', '코요태 김종민', '포미닛', '권정열 10cm', '가수 바다', '밴드 소란',
                     '범키', '그룹 씨야', '로꼬', '가수 엘시', '틴탑', '젝스키스', '유키스', '허각', '정세운', '홍대광', '가수 구피',
                     '가수 The One 더원', '혼성그룹 샵', '가수 홍자', '가수 김현철', '가수 지코', 'EXID', '가수 김민우', '황치열',
                     'B.A.P', '가수 헨리', '가수 엠투엠', '가수 키썸', '아이돌 걸스데이', '갓세븐', '가수 이태종', '브레이브걸스',
                     '악뮤 이수현', '달샤벳', '가수 린', '가수 김재환', '가수 벅 buck', '가수 원티드', '가수 이루', '헬로비너스',
                     '가수 터보', '엠씨더맥스', '가수 일레인', '가수 펀치 punch', '조관우', '유닛 씨스타19', '엑소 찬열', '김세정',
                     '레게 강 같은 평화', '가수 김종국', '가수 아웃렛', '현빈', '가수 권순관', '정다경', '유주', '가수 장나라',
                     '슈퍼주니어-K.R.Y.', '가수 퍼센트', '마마무 화사', '윤미래', '가수 김준수', np.nan, '엑소 백현', '라붐',
                     '버블 시스터즈', '아이돌 다이아',

                     # 814곡 이후 추가분
                     '김세정', '송승헌', '가수 비쥬', '국악 이송연', '아이 차윤지', '가수 배치기', '차태현', '가수 경서', '가수 임도혁',
                     '민현', '밴드 오이스터', '레드벨벳', '신승태', 'b1a4 산들', '양다일', '가수 김부용', '오현란', '차은우', '인디 이예린',
                     '힙합 그레이', 'KCM', '레드벨벳 조이', '강혜인', '가수 위수', '가수 신촌블루스', '가수 이정현', '힙합 딘', '신미래',
                     '가수 이기찬', '솔지', '가수 고 김현식', 'Ashily 애슐리', '정유지', '로이킴', '지진석', '솔로가수 강현주', '트롯 경상아가씨',
                     '이민혁', '조장혁', '오왠', '하현우', '이무진', '유승우', '장덕철', '한스밴드', '데이먼스 이어', '소유미', '가수 노을',
                     '가수 전철민', '셔누', '가수 김경호', '가수 하울', '그룹 소울스타', '서연 여름안에서', '러브홀릭 지선', '진민호',
                     '정효빈', '가수 자두', '썸데이 알고 있나요', '가수 재하', '죠지', '유니 허윤', np.nan, '가수 박다혜', '어반자카파',
                     '태무 강병준', '송윤아', '트로트 최향', '도도 널 미워하고 있어', '가수 손성훈', 'SS501', '콜드 colde', '유노윤호',
                     '태사비애', 'Luv 전혜빈', '전상근', '엑소 카이', '가수 김민석', '가수 티맥스', np.nan, '사운드 팔레트 SOUND PALETTE',
                     '러니', '조규만', '윤종신', '신예영', '박강성', '에이스타일', '그룹 일렉트로보이즈', '엑소 첸', '가수 최성수',
                     '원더나인', '피플크루', '웬디', '가수 이소정', '램씨', '어반자카파 조현아', '신용재', '이기광', '채정안', '가수 소나무 이동은',
                     '오마이걸 승희', '하하', '그룹 빅스타', '가수 김정민', '러블리즈', '가수 송민경', '가수 지코', np.nan, '권인하',
                     '가수 김한결', '노라조', '반광옥', '트롯 전국체전 보쌈', '가수 유영석', '티아라 N4', '가수 황인욱', '신미래', 'F-IV 김현수']


list_artist_dict = ['뮤직카우', '앤씨아', '견우', '이우', 'JBJ', '헤일로', '네이처', '이예준', '야다', 'AOA', '빅마마', '미교', '정동하',
                    '백지영', '민들레', '플라워', '윤상', '자이언트핑크', '에디킴', '멜로망스', '혜령', '양수경', '용주', '청하',
                    '주원', '트와이스', '주영훈', '보이비', '소찬휘', '전유나', '브라운아이드걸스', '송가인', '카라', '빈지노', '루나솔라',
                    '울랄라세션', '엠블랙', '엄정화', '하성운', 'JBJ95', '임한별', '신화', '반하나', '치타', '원투', '편승엽', '박유천',
                    '우주소녀', '베스티', 'IKON', '태용', '이하이', '에릭남', '에코브릿지', '민아', '아스트로', '아이오아이', '도끼',
                    '박장근', '유엔', '이달의소녀', '미스에스', '양파', '더 씨야', '데이브레이크', '알리', '유니티', 'CLC', '스토니 스컹크',
                    '쎄쎄쎄', '멜로디데이', 'H&D', '윤하', '쿨', '에어플레인', '루나', '정엽', '쿠시', '팀', '정미애', '강민혁', '박현빈',
                    '백아연', '몬스타엑스', '씨스타', '비투비', '이수', '장혜리', '손승연', '최백호', '패티김', '사무엘', '박효신',
                    '바이브', '이준호', '콜딘', '정은지', '조PD', '맥켈리', '신신애', '왁스', '타이거JK', '슈퍼주니어', '다이나믹 듀오',
                    '김세헌', '슈가볼', '김나영', '세븐', '애프터스쿨', '이우상', '김범수', '더 크로스', '휘성', '적재', '서인영', '엠씨더맥스',
                    '오유진', 'DJ DOC', '홍진영', '손담비', '대성', '먼데이 키즈', '윤딴딴', '선우정아', '비비', '별', '일리네어',
                    '임창정', '아이비', '성유진', '김장훈', '인피니트', '한동근', '다비치', '심신', '리사', '박원', '현아', '효린', '지드래곤',
                    '진해성', 'NS윤지', '여자친구', '자이언티', '구창모', '마이티마우스', '레디', '아이즈원', '유지', '태진아', '2NE1',
                    '아이유', '규현', '훈스', '기리보이', '화요비', '이수영', '원모어찬스', '소방차', '이승철', '김재중', '칵스',
                    'GG', '소향', '일락', '천상지희 더 그레이스', '이재훈', '벤', '지천비화', '악동뮤지션', '투개월', '전효성', '김태우',
                    '10CM', '뉴이스트', '김필', '씨잼', '고유진', '박혜경', '빅뱅', 'UV', '변진섭', '리쌍', '양혜승',
                    '유재석 X Dok2', '렉시', '티아라', '엑소', '샤이니', '백예린', '모모랜드', '광해', '키친', '용의자', '선미', '박지훈',
                    'GD&TOP', 'SG워너비', '에이프릴', '박봄', '정기고', '코요태', 'B1A4', '지아', '정인', '바비킴', '김현성', '제로',
                    '파이브돌스', '수지', '스테디', '제국의아이들', np.nan, '오투포', '강수지', '매드클라운', '비투비', '송하예',
                    '거미', '한해', '하동균', '바비', '태양', '수호', '전우성', '케이윌', '제리케이', '송지은', '루그', '크러쉬',
                    '김나희', '워너원', '크레용팝', '버즈', '조성모', '아월', '이진혁', '라디', '컨츄리꼬꼬', '마마무', '팔로알토',
                    '김보경', '아이콘', '황보', '주석', '양요섭', '애즈원', 'MC몽', '이병헌', '길구봉구', '소유', '시크릿', '메이비',
                    '지연', '일기예보', '딸기우유', '환희', '김수희', '스윙스', '드림캐쳐', '투아이즈', 'V.O.S', '박명수', '마리오',
                    '정준일', '임재범', '제시', '어쿠루브', 'god', '쏜애플', '장우혁', '스페이스 에이', '유열', 'MOBB', '도겸',
                    '알렉스', '자두', 'Lim Kim', '샘김', '빅스', '강다니엘', '이승기', '김형중', '김원준', '윙크', '가인', '박규리',
                    '조 트리오', '윤지성', '버스커 버스커', '에이핑크', '박지윤', '2PM', '박보람', '비스트', '유미', '에일리', '테이',
                    '노리플라이', '이선희', '핑클', '브라운 아이드 소울', '다니엘', 'SURL', '수퍼비', 'izi', '지나', '김종민', '포미닛',
                    '10cm', '바다', '소란', '범키', '씨야', '로꼬', '엘시', '틴탑', '젝스키스', '유키스', '허각', '정세운', '홍대광',
                    '구피', '더원', '샵', '홍자', '김현철', '지코', 'EXID', '김민우', '황치열', 'B.A.P', '헨리', '엠투엠',
                    '키썸', '걸스데이', '갓세븐', '이태종', '브레이브걸스', '이수현', '달샤벳', '린', '김재환', '벅', '원티드', '이루',
                    '헬로비너스', '터보', '엠씨더맥스', '일레인', '펀치', '조관우', '씨스타19', '찬열', '세정', '레게 강 같은 평화',
                    '김종국', '아웃렛', '현빈', '권순관', '정다경', '유주', '장나라', '슈퍼주니어-K.R.Y.', '퍼센트', '화사',
                    '윤미래', '김준수', np.nan, '백현', '라붐', '버블 시스터즈', '다이아',

                    # 814곡 이후 추가분
                    '김세정', '송승헌', '비쥬', '이송연', '아이', '배치기', '차태현', '경서', '임도혁', '민현', '오이스터', '레드벨벳',
                    '신승태', '산들', '양다일', '김부용', '오현란', '차은우', '이예린', '그레이', 'KCM', '조이', '강혜인', '위수',
                    '신촌블루스', '이정현', '딘', '신미래', '이기찬', '솔지', '김현식', '애슐리', '정유지', '로이킴', '지진석',
                    '강현주', '경상 아가씨', '이민혁', '조장혁', '오왠', '하현우', '이무진', '유승우', '장덕철', '한스밴드', '데이먼스 이어',
                    '소유미', '노을', '전철민', '셔누', '김경호', '하울', '소울스타', '서연', '지선', '진민호', '정효빈', '자두',
                    '썸데이', '재하', '죠지', '유니', np.nan, '박다혜', '어반자카파', '태무', '송윤아', '최향', '도도', '손성훈',
                    'SS501', '콜드', '유노윤호', '태사비애', 'Luv', '전상근', '카이', '김민석', '티맥스', np.nan, '사운드 팔레트',
                    '러니', '조규만', '윤종신', '신예영', '박강성', '에이 스타일', '일렉트로보이즈', '첸', '최성수', '원더나인', '피플크루',
                    '웬디', '이소정', '램씨', '조현아', '신용재', '이기광', '채정안', '이동은', '승희', '하하', '빅스타', '김정민',
                    '러블리즈', '송민경', '지코', '가시찔레', '권인하', '김한결', '노라조', '반광옥', '보쌈', '유영석', '티아라 N4', '황인욱',
                    '신미래', '김현수']



list_artist_NNP = ['뮤직카우', '앤씨아', '견우', '이우', 'JBJ', '헤일로', '네이처', '이예준', '야다', 'AOA', '빅마마', '미교', '정동하',
                    '백지영', '민들레', '플라워', '윤상', '자이언트핑크', '에디킴', '멜로망스', '혜령', '양수경', '용주', '청하',
                    '주원', '트와이스', '주영훈', '보이비', '소찬휘', '전유나', '브라운아이드걸스', '송가인', '카라', '빈지노', '루나솔라',
                    '울랄라세션', '엠블랙', '엄정화', '하성운', 'JBJ95', '임한별', '신화', '반하나', '치타', '원투', '편승엽', '박유천',
                    '우주소녀', '베스티', 'IKON', '태용', '이하이', '에릭남', '에코브릿지', '민아', '아스트로', '아이오아이', '도끼',
                    '박장근', '유엔', '이달의소녀', '미스에스', '양파', '더 씨야', '데이브레이크', '알리', '유니티', 'CLC', '스토니 스컹크',
                    '쎄쎄쎄', '멜로디데이', 'H&D', '윤하', '쿨', '에어플레인', '루나', '정엽', '쿠시', '팀', '정미애', '강민혁', '박현빈',
                    '백아연', '몬스타엑스', '씨스타', '비투비', '이수', '장혜리', '손승연', '최백호', '패티김', '사무엘', '박효신',
                    '바이브', '이준호', '콜딘', '정은지', '조PD', '맥켈리', '신신애', '왁스', '타이거JK', '슈퍼주니어', '다이나믹 듀오',
                    '김세헌', '슈가볼', '김나영', '세븐', '애프터스쿨', '이우상', '김범수', '더 크로스', '휘성', '적재', '서인영', '엠씨더맥스',
                    '오유진', 'DJ DOC', '홍진영', '손담비', '대성', '먼데이 키즈', '윤딴딴', '선우정아', '비비', '별', '일리네어',
                    '임창정', '아이비', '성유진', '김장훈', '인피니트', '한동근', '다비치', '심신', '리사', '박원', '현아', '효린', '지드래곤',
                    '진해성', 'NS윤지', '여자친구', '자이언티', '구창모', '마이티마우스', '레디', '아이즈원', '유지', '태진아', '2NE1',
                    '아이유', '규현', '훈스', '기리보이', '화요비', '이수영', '원모어찬스', '소방차', '이승철', '김재중', '칵스',
                    'GG', '소향', '일락', '천상지희 더 그레이스', '이재훈', '벤', '지천비화', '악동뮤지션', '투개월', '전효성', '김태우',
                    '10CM', '뉴이스트', '김필', '씨잼', '고유진', '박혜경', '빅뱅', 'UV', '변진섭', '리쌍', '양혜승',
                    '유재석 X Dok2', '렉시', '티아라', '엑소', '샤이니', '백예린', '모모랜드', '광해', '키친', '용의자', '선미', '박지훈',
                    'GD&TOP', 'SG워너비', '에이프릴', '박봄', '정기고', '코요태', 'B1A4', '지아', '정인', '바비킴', '김현성', '제로',
                    '파이브돌스', '수지', '스테디', '제국의아이들', '오투포', '강수지', '매드클라운', '비투비', '송하예',
                    '거미', '한해', '하동균', '바비', '태양', '수호', '전우성', '케이윌', '제리케이', '송지은', '루그', '크러쉬',
                    '김나희', '워너원', '크레용팝', '버즈', '조성모', '아월', '이진혁', '라디', '컨츄리꼬꼬', '마마무', '팔로알토',
                    '김보경', '아이콘', '황보', '주석', '양요섭', '애즈원', 'MC몽', '이병헌', '길구봉구', '소유', '시크릿', '메이비',
                    '지연', '일기예보', '딸기우유', '환희', '김수희', '스윙스', '드림캐쳐', '투아이즈', 'V.O.S', '박명수', '마리오',
                    '정준일', '임재범', '제시', '어쿠루브', 'god', '쏜애플', '장우혁', '스페이스 에이', '유열', 'MOBB', '도겸',
                    '알렉스', '자두', 'Lim Kim', '샘김', '빅스', '강다니엘', '이승기', '김형중', '김원준', '윙크', '가인', '박규리',
                    '조 트리오', '윤지성', '버스커 버스커', '에이핑크', '박지윤', '2PM', '박보람', '비스트', '유미', '에일리', '테이',
                    '노리플라이', '이선희', '핑클', '브라운 아이드 소울', '다니엘', 'SURL', '수퍼비', 'izi', '지나', '김종민', '포미닛',
                    '10cm', '바다', '소란', '범키', '씨야', '로꼬', '엘시', '틴탑', '젝스키스', '유키스', '허각', '정세운', '홍대광',
                    '구피', '더원', '샵', '홍자', '김현철', '지코', 'EXID', '김민우', '황치열', 'B.A.P', '헨리', '엠투엠',
                    '키썸', '걸스데이', '갓세븐', '이태종', '브레이브걸스', '이수현', '달샤벳', '린', '김재환', '벅', '원티드', '이루',
                    '헬로비너스', '터보', '엠씨더맥스', '일레인', '펀치', '조관우', '씨스타19', '찬열', '세정', '레게 강 같은 평화',
                    '김종국', '아웃렛', '현빈', '권순관', '정다경', '유주', '장나라', '슈퍼주니어-K.R.Y.', '퍼센트', '화사',
                    '윤미래', '김준수', '백현', '라붐', '버블 시스터즈', '다이아',

                    '김세정', '송승헌', '비쥬', '이송연', '아이', '배치기', '차태현', '경서', '임도혁', '민현', '오이스터', '레드벨벳',
                    '신승태', '산들', '양다일', '김부용', '오현란', '차은우', '이예린', '그레이', 'KCM', '조이', '강혜인', '위수',
                    '신촌블루스', '이정현', '딘', '신미래', '이기찬', '솔지', '김현식', '애슐리', '정유지', '로이킴', '지진석',
                    '강현주', '경상 아가씨', '이민혁', '조장혁', '오왠', '하현우', '이무진', '유승우', '장덕철', '한스밴드', '데이먼스 이어',
                    '소유미', '노을', '전철민', '셔누', '김경호', '하울', '소울스타', '서연', '지선', '진민호', '정효빈', '자두',
                    '썸데이', '재하', '죠지', '유니', '박다혜', '어반자카파', '태무', '송윤아', '최향', '도도', '손성훈',
                    'SS501', '콜드', '유노윤호', '태사비애', 'Luv', '전상근', '카이', '김민석', '티맥스', '사운드 팔레트',
                    '러니', '조규만', '윤종신', '신예영', '박강성', '에이 스타일', '일렉트로보이즈', '첸', '최성수', '원더나인', '피플크루',
                    '웬디', '이소정', '램씨', '조현아', '신용재', '이기광', '채정안', '이동은', '승희', '하하', '빅스타', '김정민',
                    '러블리즈', '송민경', '지코', '가시찔레', '권인하', '김한결', '노라조', '반광옥', '보쌈', '유영석', '티아라 N4', '황인욱',
                   '신미래', '김현수']

def df_nlp():
    df_artist_nlp = pd.DataFrame([list_artist_mc, list_artist_query, list_artist_dict], index=['music_cow', 'nlp_query', 'nlp_dict']).T
    # df_artist_nlp.to_pickle("./storage/df_raw_data/df_artist_nlp.pkl")

    return df_artist_nlp


if __name__ == '__main__':
    df_artist_nlp = pd.DataFrame([list_artist_mc, list_artist_query, list_artist_dict], index=['music_cow', 'nlp_query', 'nlp_dict']).T
    df_artist_nlp.to_pickle("../storage/df_raw_data/df_artist_nlp.pkl")