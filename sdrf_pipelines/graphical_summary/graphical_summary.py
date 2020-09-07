import pandas as pd


def build_relate_data(sdrf_file):
    test = []
    print('PROCESSING: ' + sdrf_file + '"')
    sdrf = pd.read_csv(sdrf_file, sep='\t')
    sdrf = sdrf.astype(str)
    sdrf.columns = map(str.lower, sdrf.columns)
    factor_cols = [c for ind, c in enumerate(sdrf) if
                   c.startswith('factor value[') and len(sdrf[c].unique()) > 1]

    if 'characteristics[biological replicate]' not in sdrf.columns:
        print("warning: no biological replicate,the default is 1")

    if 'comment[technical replicate]' not in sdrf.columns:
        print("warning: no technical replicate,the default is 1")

    if factor_cols:
        parent = index1 = 0
        f1 = list(set(sdrf[factor_cols[0]]))
        for k, v in zip(['name'] * len(f1), f1):
            test.append({k: v})
            s = sdrf[sdrf[factor_cols[0]] == v]
            if len(factor_cols) != 1:
                f2 = list(set(s[factor_cols[1]]))
                for i in f2:
                    if 'children' in test[-1]:
                        test[-1]['children'].append({'name': i})
                    else:
                        test[-1]['children'] = [{'name': i}]
                    s1 = s[s[factor_cols[1]] == i]
                    if len(factor_cols) == 2:
                        if 'characteristics[biological replicate]' not in sdrf.columns:
                            test[-1]['children'][-1]['children'] = [{'name': '1'}]
                            if 'comment[technical replicate]' not in sdrf.columns:
                                test[-1]['children'][-1]['children'][-1]['children'] = [{'name': '1'}]
                                frs = sorted(list(set(s1["comment[fraction identifier]"])))
                                for fr in frs:
                                    m = test[-1]['children'][-1]['children'][-1]['children'][-1]
                                    if 'children' in m:
                                        test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] \
                                            .append({'name': fr})
                                    else:
                                        test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] \
                                            = [{'name': fr}]
                            else:
                                trs = sorted(list(set(s1["comment[technical replicate]"])))
                                for tr in trs:
                                    if 'children' in test[-1]['children'][-1]['children'][-1]:
                                        test[-1]['children'][-1]['children'][-1]['children'].append(
                                            {'name': tr})
                                    else:
                                        test[-1]['children'][-1]['children'][-1]['children'] = [
                                            {'name': tr}]
                                    s2 = s1[s1["comment[technical replicate]"] == tr]
                                    frs = sorted(list(set(s2["comment[fraction identifier]"])))
                                    for fr in frs:
                                        m = test[-1]['children'][-1]['children'][-1]['children'][-1]
                                        if 'children' in m:
                                            test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                'children'].append(
                                                {'name': fr})
                                        else:
                                            test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                'children'] = [{'name': fr}]
                        else:
                            brs = sorted(list(set(s1['characteristics[biological replicate]'])))
                            for br in brs:
                                if 'children' in test[-1]['children'][-1]:
                                    test[-1]['children'][-1]['children'].append({'name': br})
                                else:
                                    test[-1]['children'][-1]['children'] = [{'name': br}]
                                if 'comment[technical replicate]' not in sdrf.columns:
                                    test[-1]['children'][-1]['children'][-1]['children'] = [{'name': '1'}]
                                    s2 = s1[s1['characteristics[biological replicate]'] == br]
                                    frs = sorted(list(set(s2["comment[fraction identifier]"])))
                                    for fr in frs:
                                        m = test[-1]['children'][-1]['children'][-1]['children'][-1]
                                        if 'children' in m:
                                            test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                'children'].append(
                                                {'name': fr})
                                        else:
                                            test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                'children'] = [{'name': fr}]
                                else:
                                    s2 = s1[s1['characteristics[biological replicate]'] == br]
                                    trs = sorted(list(set(s2["comment[technical replicate]"])))
                                    for tr in trs:
                                        if 'children' in test[-1]['children'][-1]['children'][-1]:
                                            test[-1]['children'][-1]['children'][-1]['children'] \
                                                .append({'name': tr})
                                        else:
                                            test[-1]['children'][-1]['children'][-1]['children'] = [
                                                {'name': tr}]
                                        s3 = s2[s2["comment[technical replicate]"] == tr]
                                        frs = sorted(list(set(s3["comment[fraction identifier]"])))
                                        for fr in frs:
                                            m = test[-1]['children'][-1]['children'][-1]['children'][-1]
                                            if 'children' in m:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                    'children'] \
                                                    .append({'name': fr})
                                            else:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                    'children'] = [
                                                    {'name': fr}]

                    else:
                        f3 = list(set(s1[factor_cols[2]]))
                        for j in f3:
                            if 'children' in test[-1]['children'][-1]:
                                test[-1]['children'][-1]['children'].append({'name': j})
                            else:
                                test[-1]['children'][-1]['children'] = [{'name': j}]
                            s2 = s1[s1[factor_cols[2]] == j]
                            if len(factor_cols) == 3:
                                if 'characteristics[biological replicate]' not in sdrf.columns:
                                    test[-1]['children'][-1]['children'][-1]['children'] = [{'name': '1'}]
                                    if 'comment[technical replicate]' not in sdrf.columns:
                                        test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] = [
                                            {'name': '1'}]
                                        frs = sorted(list(set(s2["comment[fraction identifier]"])))
                                        for fr in frs:
                                            m = test[-1]['children'][-1]['children'][-1]['children'][-1]['children'][-1]
                                            if 'children' in m:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1]['children'][
                                                    -1]['children'].append({'name': fr})
                                            else:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1]['children'][
                                                    -1]['children'] = [{'name': fr}]
                                    else:
                                        trs = sorted(list(set(s2["comment[technical replicate]"])))
                                        for tr in trs:
                                            if 'children' in test[-1]['children'][-1]['children'][-1]['children'][-1]:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                    'children'].append({'name': tr})
                                            else:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] = [
                                                    {'name': tr}]
                                            s3 = s2[s2["comment[technical replicate]"] == tr]
                                            frs = sorted(list(set(s3["comment[fraction identifier]"])))
                                            for fr in frs:
                                                if test[-1]['children'][-1]['children'][-1]['children'][-1]['children'][
                                                    -1]:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'].append(
                                                        {'name': fr})
                                                else:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'] = [{'name': fr}]
                                else:
                                    brs = sorted(list(set(s2['characteristics[biological replicate]'])))
                                    for br in brs:
                                        if 'children' in test[-1]['children'][-1]['children'][-1]:
                                            test[-1]['children'][-1]['children'][-1]['children'].append({'name': br})
                                        else:
                                            test[-1]['children'][-1]['children'][-1]['children'] = [{'name': br}]
                                        if 'comment[technical replicate]' not in sdrf.columns:
                                            test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] \
                                                = [{'name': '1'}]
                                            s3 = s2[s2['characteristics[biological replicate]'] == br]
                                            frs = sorted(list(set(s3["comment[fraction identifier]"])))
                                            for fr in frs:
                                                m = \
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]
                                                if 'children' in m:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'].append(
                                                        {'name': fr})
                                                else:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'] \
                                                        = [{'name': fr}]
                                        else:
                                            s3 = s2[s2['characteristics[biological replicate]'] == br]
                                            trs = sorted(list(set(s3["comment[technical replicate]"])))
                                            for tr in trs:
                                                m = test[-1]['children'][-1]['children'][-1]['children'][-1]
                                                if 'children' in m:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] \
                                                        .append({'name': tr})
                                                else:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'] = [{'name': tr}]
                                                s4 = s3[s3["comment[technical replicate]"] == tr]
                                                frs = sorted(list(set(s4["comment[fraction identifier]"])))
                                                for fr in frs:
                                                    m = test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]
                                                    if 'children' in m:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'] \
                                                            .append({'name': fr})
                                                    else:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'] = [
                                                            {'name': fr}]
                            else:
                                f4 = list(set(s2[factor_cols[3]]))
                                for e in f4:
                                    if 'children' in test[-1]['children'][-1]['children'][-1]:
                                        test[-1]['children'][-1]['children'][-1]['children'].append({'name': j})
                                    else:
                                        test[-1]['children'][-1]['children'][-1]['children'] = [{'name': j}]
                                    s3 = s2[s2[factor_cols[3]] == e]
                                    if 'characteristics[biological replicate]' not in sdrf.columns:
                                        test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] = [
                                            {'name': '1'}]
                                        if 'comment[technical replicate]' not in sdrf.columns:
                                            test[-1]['children'][-1]['children'][-1]['children'][-1]['children'][-1][
                                                'children'] \
                                                = [{'name': '1'}]
                                            frs = sorted(list(set(s3["comment[fraction identifier]"])))
                                            for fr in frs:
                                                m = \
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][
                                                        -1]['children'][-1]
                                                if 'children' in m:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'][-1]['children']. \
                                                        append({'name': fr})
                                                else:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'][-1]['children'] = [{'name': fr}]
                                        else:
                                            trs = sorted(list(set(s3["comment[technical replicate]"])))
                                            for tr in trs:
                                                m = \
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]
                                                if 'children' in m:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'].append({'name': tr})
                                                else:
                                                    test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'] = [{'name': tr}]
                                                s4 = s3[s3["comment[technical replicate]"] == tr]
                                                frs = sorted(list(set(s4["comment[fraction identifier]"])))
                                                for fr in frs:
                                                    m = test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'][-1]
                                                    if 'children' in m:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'][-1]['children']. \
                                                            append({'name': fr})
                                                    else:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'][-1]['children'] = [{'name': fr}]
                                    else:
                                        brs = sorted(list(set(s3['characteristics[biological replicate]'])))
                                        for br in brs:
                                            if 'children' in test[-1]['children'][-1]['children'][-1]['children'][-1]:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                    'children'].append({'name': br})
                                            else:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1]['children'] = [
                                                    {'name': br}]
                                            if 'comment[technical replicate]' not in sdrf.columns:
                                                test[-1]['children'][-1]['children'][-1]['children'][-1]['children'][
                                                    -1]['children'] = [{'name': '1'}]
                                                s4 = s3[s3['characteristics[biological replicate]'] == br]
                                                frs = sorted(list(set(s4["comment[fraction identifier]"])))
                                                for fr in frs:
                                                    m = test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][-1]['children'][-1]
                                                    if 'children' in m:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'][-1]['children']. \
                                                            append({'name': fr})
                                                    else:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'][-1]['children'] = [{'name': fr}]
                                            else:
                                                s4 = s3[s3['characteristics[biological replicate]'] == br]
                                                trs = sorted(list(set(s4["comment[technical replicate]"])))
                                                for tr in trs:
                                                    m = test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                        'children'][
                                                        -1]
                                                    if 'children' in m:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'].append({'name': tr})
                                                    else:
                                                        test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'] = [{'name': tr}]
                                                    s5 = s4[s4["comment[technical replicate]"] == tr]
                                                    frs = sorted(list(set(s5["comment[fraction identifier]"])))
                                                    for fr in frs:
                                                        m = test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                            'children'][-1]['children'][-1]
                                                        if 'children' in m:
                                                            test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                                'children'][-1]['children'][-1]['children']. \
                                                                append({'name': fr})
                                                        else:
                                                            test[-1]['children'][-1]['children'][-1]['children'][-1][
                                                                'children'][-1]['children'][-1]['children'] = [
                                                                {'name': fr}]
            else:
                if 'characteristics[biological replicate]' not in sdrf.columns:
                    test[-1]['children'] = [{'name': '1'}]
                    if 'comment[technical replicate]' not in sdrf.columns:
                        test[-1]['children'][-1]['children'] = [{'name': '1'}]
                        frs = sorted(list(set(s["comment[fraction identifier]"])))
                        for fr in frs:
                            if 'children' in test[-1]['children'][-1]['children'][-1]:
                                test[-1]['children'][-1]['children'][-1]['children'].append({'name': fr})
                            else:
                                test[-1]['children'][-1]['children'][-1]['children'] = [{'name': fr}]
                    else:
                        trs = sorted(list(set(s["comment[technical replicate]"])))
                        for tr in trs:
                            if 'children' in test[-1]['children'][-1]:
                                test[-1]['children'][-1]['children'].append({'name': tr})
                            else:
                                test[-1]['children'][-1]['children'] = [{'name': tr}]
                            s1 = s[s["comment[technical replicate]"] == tr]
                            frs = sorted(list(set(s1["comment[fraction identifier]"])))
                            for fr in frs:
                                if 'children' in test[-1]['children'][-1]['children'][-1]:
                                    test[-1]['children'][-1]['children'][-1]['children'].append({'name': fr})
                                else:
                                    test[-1]['children'][-1]['children'][-1]['children'] = [{'name': fr}]
                else:
                    brs = sorted(list(set(s['characteristics[biological replicate]'])))
                    for br in brs:
                        if 'children' in test[-1]:
                            test[-1]['children'].append({'name': br})
                        else:
                            test[-1]['children'] = [{'name': br}]
                        if 'comment[technical replicate]' not in sdrf.columns:
                            test[-1]['children'][-1]['children'] = [{'name': '1'}]
                            s1 = s[s['characteristics[biological replicate]'] == br]
                            frs = sorted(list(set(s1["comment[fraction identifier]"])))
                            for fr in frs:
                                if 'children' in test[-1]['children'][-1]['children'][-1]:
                                    test[-1]['children'][-1]['children'][-1]['children'].append({'name': fr})
                                else:
                                    test[-1]['children'][-1]['children'][-1]['children'] = [{'name': fr}]
                        else:
                            s1 = s[s['characteristics[biological replicate]'] == br]
                            trs = sorted(list(set(s1["comment[technical replicate]"])))
                            for tr in trs:
                                if 'children' in test[-1]['children'][-1]:
                                    test[-1]['children'][-1]['children'].append({'name': tr})
                                else:
                                    test[-1]['children'][-1]['children'] = [{'name': tr}]
                                s2 = s1[s1["comment[technical replicate]"] == tr]
                                frs = sorted(list(set(s2["comment[fraction identifier]"])))
                                for fr in frs:
                                    if 'children' in test[-1]['children'][-1]['children'][-1]:
                                        test[-1]['children'][-1]['children'][-1]['children'].append({'name': fr})
                                    else:
                                        test[-1]['children'][-1]['children'][-1]['children'] = [{'name': fr}]
    else:
        if 'characteristics[biological replicate]' not in sdrf.columns:
            test = [{'name': '1'}]
            if 'comment[technical replicate]' not in sdrf.columns:
                test[-1]['children'] = [{'name': '1'}]
                frs = sorted(list(set(sdrf["comment[fraction identifier]"])))
                for fr in frs:
                    if 'children' in test[-1]['children'][-1]:
                        test[-1]['children'][-1]['children'].append({'name': fr})
                    else:
                        test[-1]['children'][-1]['children'] = [{'name': fr}]
            else:
                trs = sorted(list(set(sdrf["comment[technical replicate]"])))
                for tr in trs:
                    if 'children' in test[-1]:
                        test[-1]['children'].append({'name': tr})
                    else:
                        test[-1]['children'] = [{'name': tr}]
                    s = sdrf[sdrf["comment[technical replicate]"] == tr]
                    frs = sorted(list(set(s["comment[fraction identifier]"])))
                    for fr in frs:
                        if 'children' in test[-1]['children'][-1]:
                            test[-1]['children'][-1]['children'].append({'name': fr})
                        else:
                            test[-1]['children'][-1]['children'] = [{'name': fr}]
        else:
            brs = sorted(list(set(sdrf['characteristics[biological replicate]'])))
            for br in brs:
                test.append({'name': br})
                s = sdrf[sdrf['characteristics[biological replicate]'] == br]
                if 'comment[technical replicate]' not in sdrf.columns:
                    test[-1]['children'] = [{'name': '1'}]
                    frs = sorted(list(set(s["comment[fraction identifier]"])))
                    for fr in frs:
                        if 'children' in test[-1]['children'][-1]:
                            test[-1]['children'][-1]['children'].append({'name': fr})
                        else:
                            test[-1]['children'][-1]['children'] = [{'name': fr}]
                else:
                    trs = sorted(list(set(s["comment[technical replicate]"])))
                    for tr in trs:
                        if 'children' in test[-1]:
                            test[-1]['children'].append({'name': tr})
                        else:
                            test[-1]['children'] = [{'name': tr}]
                        s1 = s[s["comment[technical replicate]"] == tr]
                        frs = sorted(list(set(s1["comment[fraction identifier]"])))
                        for fr in frs:
                            if 'children' in test[-1]['children'][-1]:
                                test[-1]['children'][-1]['children'].append({'name': fr})
                            else:
                                test[-1]['children'][-1]['children'] = [{'name': fr}]

    return test


def generate_graphical_summary(sdrf_file, output_file):
    sdrf = pd.read_csv(sdrf_file, sep='\t')
    sdrf = sdrf.astype(str)
    sdrf.columns = map(str.lower, sdrf.columns)
    factor_cols = [c for ind, c in enumerate(sdrf) if
                   c.startswith('factor value[') and len(sdrf[c].unique()) > 1]
    data = build_relate_data(sdrf_file)
    if len(factor_cols) == 0:
        subtitle = "First Level: biological replicate \\n\\n" + "Second Level: technical replicate \\n\\n" + \
                   "Last Level: Fraction"
    elif len(factor_cols) == 1:
        subtitle = "First Level:" + factor_cols[0] + "\\n\\n" + \
                   "Second Level: biological replicate \\n\\n" + "Third Level: technical replicate \\n\\n" + \
                   "Last level: Fraction"

    elif len(factor_cols) == 2:
        subtitle = "First Level: " + factor_cols[0] + "\\n\\n" + \
                   "Second Level: " + factor_cols[1] + "\\n\\n" + \
                   "Third Level: biological replicate \\n\\n" + "Fourth Level: technical replicate \\n\\n" + \
                   "Last level: Fraction"
    elif len(factor_cols) == 3:
        subtitle = "First Level: " + factor_cols[0] + "\\n\\n" + \
                   "Second Level: " + factor_cols[1] + "\\n\\n" + \
                   "Third Level: " + factor_cols[2] + "\\n\\n" + \
                   "Fourth Level: biological replicate \\n\\n" + "Fifth Level: technical replicate \\n\\n" + \
                   "Last level: Fraction"
    else:
        subtitle = "First Level: " + factor_cols[0] + "\\n\\n" + \
                   "Second Level: " + factor_cols[1] + "\\n\\n" + \
                   "Third Level: " + factor_cols[2] + "\\n\\n" + \
                   "Fourth Level: " + factor_cols[3] + "\\n\\n" + \
                   "Fifth Level: biological replicate \\n\\n" + "Sixth Level: technical replicate \\n\\n" + \
                   "Last level: Fraction"

    data = [{'children': data, 'name': 'SDRF'}]

    message = """<!DOCTYPE html>
    <html>
    <head>
    <meta charset="UTF-8">
    <title>SDRF Graphical summary</title>
            <script type="text/javascript" src="https://assets.pyecharts.org/assets/echarts.min.js"></script>
    
    </head>
    <body>
    <div id="4c19e92110404b2aaf9e62a096e146d0" class="chart-container" style="width:100%s; height:600px;"></div>
    <script>
        var chart_4c19e92110404b2aaf9e62a096e146d0 = echarts.init(
            document.getElementById('4c19e92110404b2aaf9e62a096e146d0'), 'white', {renderer: 'canvas'});
        var option_4c19e92110404b2aaf9e62a096e146d0 = {
    "animation": true,
    "animationThreshold": 2000,
    "animationDuration": 1000,
    "animationEasing": "cubicOut",
    "animationDelay": 0,
    "animationDurationUpdate": 300,
    "animationEasingUpdate": "cubicOut",
    "animationDelayUpdate": 0,
    "color": [
        "#c23531",
        "#2f4554",
        "#61a0a8",
        "#d48265",
        "#749f83",
        "#ca8622",
        "#bda29a",
        "#6e7074",
        "#546570",
        "#c4ccd3",
        "#f05b72",
        "#ef5b9c",
        "#f47920",
        "#905a3d",
        "#fab27b",
        "#2a5caa",
        "#444693",
        "#726930",
        "#b2d235",
        "#6d8346",
        "#ac6767",
        "#1d953f",
        "#6950a1",
        "#918597"
    ],
    "series": [
        {
            "type": "tree",
            "data": """ + str(data) + """, "symbol": "arrow", "symbolSize": 7, "roam": false, "expandAndCollapse": 
            true, "initialTreeDepth": 1, "layout": "orthogonal", "orient": "TB", "label": { "show": true, "position": 
            "top", "margin": 8 }, "leaves": { "label": { "show": true, "position": "top", "margin": 8 } } } ], 
            "legend": [ { "data": [], "selected": {}, "show": true, "padding": 5, "itemGap": 10, "itemWidth": 25, 
            "itemHeight": 14 } ], "tooltip": { "show": true, "trigger": "item", "triggerOn": "mousemove|click", 
            "axisPointer": { "type": "line" }, "textStyle": { "fontSize": 14 }, "borderWidth": 0 }, "title": [ { 
            "text": "SDRF Graphical summary", 
            "subtext": \"""" + str(subtitle) + """\", 
            "padding": 5, "itemGap": 50, "subtextStyle": { "color": "#175", "font_style": "oblique" } } ] }; 
            chart_4c19e92110404b2aaf9e62a096e146d0.setOption(option_4c19e92110404b2aaf9e62a096e146d0); </script> 
            </body> </html> """

    f = open(output_file, 'w')
    f.write(message)
    f.close()
    print("Successfully generated")
