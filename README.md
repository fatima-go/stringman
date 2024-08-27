# Golang text support manager #

프로그램상에서 다루는 다양한 텍스트는 보통 소스코드에 선언되어 있을텐데, 이것을 XML 파일 형태로 별도 운영하는 방법을 제공한다

## xml 파일 샘플

```xml
<query>
    <!--
    2매 이상 등록 & 동행자 좌석 지정을 안한 경우 (CI 존재 할 경우)
    -->
    <text id="BuildTicketUnavailableWithCI">
        <![CDATA[%s님, 동행자 좌석을 지정해 주세요.

· 좌석이 모두 지정되어야 티켓 발권이 가능해요.
· 좌석 지정은 [마이페이지 > 예매내역 상세]에서 할 수 있어요.

▶상품명 : %s
▶예매번호 : %s (총 %s매)
▶관람일시 : %s

※ 나의 예매내역 보기
%s]]>
    </text>
</query>
```

# stringman code Sample #

```go
xmlFileDir := GetFatimaRuntime().GetEnv().GetFolderGuide().GetAppFolder()
pref := stringman.NewStringmanPreference(xmlFileDir)
pref.Fileset = "text*.xml"

man, err := stringman.NewStringman(pref)
if err != nil {
log.Error("fail to create stringman : %s", err.Error())
return
}

textManager = man
log.Info("%s", man)
```




