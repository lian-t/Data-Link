
let polipop = new Polipop('mypolipop', {
    closer: false,
    position: 'center',
    layout: 'popups',
    insert: 'before',
    life: 4000,
    pool: 0,
    sticky: false,
    pauseOnHover: true
});

function showPop(type, msg, reload) {
    if (type === 'success') {
        polipop.add({
            type: 'success',
            title: '提示',
            content: msg + '成功',
            close: function (notification, element) {
                if (reload) {
                    reloadTask()
                }
            }
        });
        return
    }
    if (type === 'error') {
        polipop.add({
            type: 'error',
            title: '提示',
            content: msg + '失败'
        });
    }
}

function formatDate(ts) {
    const time = new Date(parseInt(ts) * 1000);
    const y = time.getFullYear();  //年
    let m = time.getMonth() + 1;  //月
    if (m < 10) {
        m = '0' + m
    }
    let d = time.getDate();  //日
    if (d < 10) {
        d = '0' + d
    }
    let h = time.getHours();  //时
    if (h < 10) {
        h = '0' + h
    }
    let mm = time.getMinutes();  //分
    if (mm < 10) {
        mm = '0' + mm
    }
    let s = time.getSeconds();  //秒
    if (s < 10) {
        s = '0' + s
    }
    return y + "-" + m + "-" + d + " " + h + ":" + mm + ":" + s;
}