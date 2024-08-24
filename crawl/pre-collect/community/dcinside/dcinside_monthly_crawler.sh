#!/bin/bash

# 입력된 인수를 변수에 할당
query=$1
start_date=$2
end_date=$3

# 달의 최대 일 수를 case문으로 처리
max_days_in_month() {
    case $1 in
        01|03|05|07|08|10|12) echo 31 ;;
        04|06|09|11) echo 30 ;;
        02)
            if [[ $(( $2 % 4 )) -eq 0 && ( $(( $2 % 100 )) -ne 0 || $(( $2 % 400 )) -eq 0 ) ]]; then
                echo 29
            else
                echo 28
            fi
            ;;
    esac
}


echo "$query $start_date $end_date" >> batch.log

mkdir -p result

# 초기 변수 설정
start_year=$(echo $start_date | cut -d'-' -f1)
start_month=$(echo $start_date | cut -d'-' -f2 | sed 's/^0*//')
start_day=$(echo $start_date | cut -d'-' -f3 | cut -d' ' -f1)
start_time=$(echo $start_date | cut -d' ' -f2)

end_year=$(echo $end_date | cut -d'-' -f1)
end_month=$(echo $end_date | cut -d'-' -f2 | sed 's/^0*//')
end_day=$(echo $end_date | cut -d'-' -f3 | cut -d' ' -f1)
end_time=$(echo $end_date | cut -d' ' -f2)

current_year=$start_year
current_month=$start_month
current_day=$start_day

# 메인 루프
while (( current_year*100 + current_month <= end_year*100 + end_month )); do
    max_day=$(max_days_in_month $(printf "%02d" $current_month) $current_year)
    
    # 첫 달에 대한 처리
    if [[ "$current_year" == "$start_year" && "$current_month" == "$start_month" ]]; then
        first_day="$current_day"
        first_time="$start_time"
    else
        first_day="01"
        first_time="00:00"
    fi
    
    # 마지막 달에 대한 처리
    if [[ "$current_year" == "$end_year" && "$current_month" == "$end_month" ]]; then
        last_day="$end_day"
        last_time="$end_time"
    else
        last_day="$max_day"
        last_time="23:59"
    fi
    
    # 변수로 나눠서 명령어 구성
    # year_month=$(echo $current_year | cut -c3-)-$(printf "%02d" $current_month)
    year_month=$current_year-$(printf "%02d" $current_month)
    start_datetime="$year_month-$first_day $first_time"
    end_datetime="$year_month-$last_day $last_time"

    # 출력
    echo "Running: python3 dcinside_crawler.py \"$query\" \"$start_datetime\" \"$end_datetime\"" >> batch.log
    echo "Running: python3 dcinside_crawler.py \"$query\" \"$start_datetime\" \"$end_datetime\""
    python3 dcinside_crawler.py "$query" "$start_datetime" "$end_datetime"
    
    # 다음 달로 이동
    if (( current_month == 12 )); then
        current_month=1
        current_year=$((current_year + 1))
    else
        current_month=$((current_month + 1))
    fi
done

####### csv 파일 병합 #######
mkdir -p merged
first_file=true
merge_file="dcinside_${query}_$(echo $start_date | sed 's/ /-/g' | sed 's/:/-/g')_$(echo $end_date | sed 's/ /-/g' | sed 's/:/-/g').csv"
for csv_file in result/dcinside_${query}*.csv; do
    if $first_file; then
        # 첫 파일의 헤더 포함
        cat "$csv_file" > "merged/$merge_file"
        first_file=false
    else
        # 이후 파일의 헤더 제외하고 병합
        tail -n +2 "$csv_file" >> "merged/$merge_file"
    fi
done

echo "모든 CSV 파일이 $merge_file 파일로 병합되었습니다."
