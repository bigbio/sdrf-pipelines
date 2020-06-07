import pandas as pd
from pyecharts.charts import Tree
from pyecharts import options as opts


def build_relate_data(sdrf_file):
    data = []

    print('PROCESSING: ' + sdrf_file + '"')
    sdrf = pd.read_csv(sdrf_file, sep='\t')
    sdrf = sdrf.astype(str)
    sdrf.columns = map(str.lower, sdrf.columns)
    global factor_cols
    factor_cols = [c for ind, c in enumerate(sdrf) if
                   c.startswith('factor value[') and len(sdrf[c].unique()) > 1]
    global tag
    tag = len(factor_cols)

    for index, row in sdrf.iterrows():
        if 'characteristics[biological replicate]' in sdrf.columns:
            if row['characteristics[biological replicate]'] == 'not available' \
                    or row['characteristics[biological replicate]'] == 'not applicable':
                row['characteristics[biological replicate]'] = '1'

        j = 1
        if tag == 1:
            if data == list():
                data.append({'name': row[factor_cols[0]]})
                if 'characteristics[biological replicate]' in sdrf.columns:
                    data[0]['children'] = [{'name': row['characteristics[biological replicate]']}]
                    if 'comment[technical replicate]' in sdrf.columns:
                        data[0]['children'][0]['children'] = [{'name': row[
                            "comment[technical replicate]"]}]
                    else:
                        data[0]['children'][0]['children'] = [{'name': '1'}]
                else:
                    data[0]['children'] = [{'name': '1'}]
                    if 'comment[technical replicate]' in sdrf.columns:
                        data[0]['children'][0]['children'] = [{'name': row[
                            "comment[technical replicate]"]}]
                    else:
                        data[0]['children'][0]['children'] = [{'name': '1'}]

                data[0]['children'][0]['children'][0]['children'] = [{'name': row["comment[fraction identifier]"]}]

            elif row[factor_cols[0]] in [i["name"] for i in data]:
                index = [i['name'] for i in data].index(row[factor_cols[0]])
                if 'characteristics[biological replicate]' in sdrf.columns:
                    if row['characteristics[biological replicate]'] in [i['name'] for i in
                                                                        data[index]['children']]:
                        index2 = [i['name'] for i in data[index]['children']].index(
                            row['characteristics[biological replicate]'])
                        if 'comment[technical replicate]' in sdrf.columns:
                            if row['comment[technical replicate]'] in [i['name'] for i in
                                                                       data[index]['children'][index2]['children']]:
                                index3 = [i['name'] for i in
                                          data[index]['children'][index2]['children']].index(
                                    row['comment[technical replicate]'])
                                data[index]['children'][index2]['children'][index3]['children'].append(
                                    {'name': row['comment[fraction identifier]']})
                            else:
                                data[index]['children'][index2]['children'] = [{'name': 1}]
                                data[index]['children'][index2]['children'][0]['children'] = \
                                    [{'name': row['comment[fraction identifier]']}]
                        else:
                            data[index]['children'][index2]['children'] = [{'name': 1}]
                            data[index]['children'][index2]['children'][0]['children'].append(
                                {'name': row['comment[fraction identifier]']})
                    else:
                        data[index]['children'].append(
                            {'name': row['characteristics[biological replicate']})
                        data[index]['children'][-1]['children'] = [
                            {'name': row['comment[technical replicate']}]
                        data[index]['children'][-1]['children'][0]['children'] = [
                            {'name': row['comment[fraction identifier]']}]

                else:
                    if 'comment[technical replicate]' in sdrf.columns:
                        if row['comment[technical replicate]'] in [i['name'] for i in
                                                                   data[index]['children'][0][
                                                                       'children']]:
                            index3 = [i['name'] for i in data[index]['children'][0]['children']].index(
                                row['comment[technical replicate]'])
                            data[index]['children'][0]['children'][index3]['children'].append(
                                {'name': row['comment[fraction identifier]']})
                        else:
                            data[index]['children'][0]['children'].append(
                                {'name': row['comment[technical replicate]']})
                            data[index]['children'][0]['children'][-1]['children'] = [
                                {'name': row['comment[fraction identifier]']}]

                    else:
                        if row['comment[fraction identifier]'] in [i['name'] for i in
                                                                   data[index]['children'][0]['children'][0][
                                                                       'children']]:
                            pass
                        else:
                            data[index]['children'][0]['children'][0]['children'].append(
                                {'name': row['comment[fraction identifier]']})
            else:
                data.append({'name': row[factor_cols[0]]})
                if 'characteristics[biological replicate]' in sdrf.columns:
                    data[-1]['children'] = [{'name': row['characteristics[biological replicate]']}]
                else:
                    data[-1]['children'] = [{'name': '1'}]

                if 'comment[technical replicate]' in sdrf.columns:
                    data[-1]['children'][0]['children'] = [{'name': row['comment[technical replicate]']}]
                else:
                    data[-1]['children'][0]['children'] = [{'name': '1'}]

                data[-1]['children'][0]['children'][0]['children'] = [
                    {'name': row['comment[fraction identifier]']}]

        else:
            while j < tag:
                if data == list():
                    data.append({'name': row[factor_cols[0]]})
                    data[0]['children'] = [{'name': row[factor_cols[j]]}]
                    if 'characteristics[biological replicate]' in sdrf.columns:
                        data[0]['children'][0]['children'] = [{'name': row['characteristics[biological replicate]']}]
                        if 'comment[technical replicate]' in sdrf.columns:
                            data[0]['children'][0]['children'][0]['children'] = [{'name': row[
                                "comment[technical replicate]"]}]
                        else:
                            data[0]['children'][0]['children'][0]['children'] = [{'name': '1'}]

                    else:
                        data[0]['children'][0]['children'] = [{'name': '1'}]
                        if 'comment[technical replicate]' in sdrf.columns:
                            data[0]['children'][0]['children'][0]['children'] = [{'name': row[
                                "comment[technical replicate]"]}]
                        else:
                            data[0]['children'][0]['children'][0]['children'] = [{'name': '1'}]

                    data[0]['children'][0]['children'][0]['children'][0]['children'] = [
                        {'name': row["comment[fraction identifier]"]}]

                elif row[factor_cols[j - 1]] in [i["name"] for i in data]:
                    index = [i['name'] for i in data].index(row[factor_cols[j - 1]])
                    if row[factor_cols[j]] in [i['name'] for i in data[index]['children']]:
                        index_1 = [i['name'] for i in data[index]['children']].index(row[factor_cols[j]])
                        if 'characteristics[biological replicate]' in sdrf.columns:
                            if row['characteristics[biological replicate]'] in [i['name'] for i in
                                                                                data[index]['children'][index_1][
                                                                                    'children']]:
                                index2 = [i['name'] for i in data[index]['children'][index_1]['children']].index(
                                    row['characteristics[biological replicate]'])
                                if 'comment[technical replicate]' in sdrf.columns:
                                    if row['comment[technical replicate]'] in [i['name'] for i in
                                                                               data[index]['children'][index_1][
                                                                                   'children'][
                                                                                   index2]['children']]:
                                        index3 = [i['name'] for i in
                                                  data[index]['children'][index_1]['children'][index2][
                                                      'children']].index(
                                            row['comment[technical replicate]'])
                                        data[index]['children'][index_1]['children'][index2]['children'][index3][
                                            'children'].append(
                                            {'name': row['comment[fraction identifier]']})
                                    else:
                                        data[index]['children'][index_1]['children'][0]['children'].append(
                                            {'name': row['comment[technical replicate]']})
                                        data[index]['children'][index_1]['children'][0]['children'][-1][
                                            'children'] = [{'name': row['comment[fraction identifier]']}]
                                else:
                                    data[index]['children'][index_1]['children'][
                                        0]['children'] = [{'name': 1}]
                                    data[index]['children'][index_1]['children'][0]['children'][0][
                                        'children'].append(
                                        {'name': row['comment[fraction identifier]']})

                            else:
                                data[index]['children'][index_1]['children'].append(
                                    {'name': row['characteristics[biological replicate]']})
                                data[index]['children'][index_1]['children'][-1]['children'] = [
                                    {'name': row['comment[technical replicate]']}]
                                data[index]['children'][index_1]['children'][-1]['children'][0]['children'] = [
                                    {'name': row['comment[fraction identifier]']}]
                        else:
                            if 'comment[technical replicate]' in sdrf.columns:
                                if row['comment[technical replicate]'] in [i['name'] for i in
                                                                           data[index]['children'][index_1]['children'][
                                                                               0][
                                                                               'children']]:
                                    index3 = [i['name'] for i in
                                              data[index]['children'][index_1]['children'][0]['children']].index(
                                        row['comment[technical replicate]'])
                                    data[index]['children'][index_1]['children'][0]['children'][index3][
                                        'children'].append(
                                        {'name': row['comment[fraction identifier]']})
                                else:
                                    data[index]['children'][index_1]['children'][0]['children'].append(
                                        {'name': row['comment[technical replicate]']})
                                    data[index]['children'][index_1]['children'][0]['children'][-1]['children'] = [
                                        {'name': row['comment[fraction identifier]']}]

                            else:
                                if row['comment[fraction identifier]'] in [
                                    i['name'] for i in
                                    data[index]['children'][index_1]['children'][0]['children'][0]['children']
                                ]:
                                    pass
                                else:
                                    data[index]['children'][index_1]['children'][0]['children'][0]['children'].append(
                                        {'name': row['comment[fraction identifier]']})

                    else:
                        data[index]['children'].append({'name': row[factor_cols[j]]})
                        data[index]['children'][-1]['children'] = [{'name': '1'}]
                        data[index]['children'][-1]['children'][0]['children'] = [{'name': '1'}]
                        data[index]['children'][-1]['children'][0]['children'][0]['children'] = [{'name': '1'}]

                else:
                    data.append({'name': row[factor_cols[j - 1]]})
                    data[-1]['children'] = [{'name': row[factor_cols[j]]}]
                    if 'characteristics[biological replicate]' in sdrf.columns:
                        data[-1]['children'][0]['children'] = [{'name': row['characteristics[biological replicate]']}]
                    else:
                        data[-1]['children'][0]['children'] = [{'name': '1'}]
                    if 'comment[technical replicate]' in sdrf.columns:
                        data[-1]['children'][0]['children'][0]['children'] = [
                            {'name': row['comment[technical replicate]']}]
                    else:
                        data[-1]['children'][0]['children'][0]['children'] = [{'name': '1'}]
                    data[-1]['children'][0]['children'][0]['children'][0]['children'] = [
                        {'name': row['comment[fraction identifier]']}]

                j += 1

    return data


def generate_graphical_summary(sdrf_file, output_file):
    page_title = 'SDRF Graphical summary'
    title = 'SDRF Graphical summary'
    data = build_relate_data(sdrf_file)

    if tag == 1:
        subtitle = 'First Level:' + factor_cols[0] + '\n\n' + \
                   'Second Level: biological replicate \n\n' + 'Third Level: technical replicate \n\n' + \
                   'Last level: Fraction'

        data = [{'children': data, 'name': 'PMID25043054'}]
        t = Tree(opts.InitOpts(page_title="PMID25043054", width="100%", height="600px", ))
        t.set_global_opts(
            title_opts=opts.TitleOpts(title="PMID25043054 Proteomics \n experiemental design",
                                      subtitle=subtitle, item_gap=50,
                                      subtitle_textstyle_opts={'color': '#175', 'font_style': 'oblique'}),
        )
    else:
        subtitle = 'First Level: ' + factor_cols[0] + '\n\n' + \
                   'Second Level: ' + factor_cols[1] + '\n\n' + \
                   'Third Level: biological replicate \n\n' + 'Fouth Level: technical replicate \n\n' + \
                   'Last level: Fraction'

        data = [{'children': data, 'name': 'SDRF'}]  # temp name
        t = Tree(opts.InitOpts(page_title=page_title, width="100%", height="600px", ))
        t.set_global_opts(title_opts=opts.TitleOpts(title=title,
                                                    subtitle=subtitle, item_gap=50,
                                                    subtitle_textstyle_opts={'color': '#175',
                                                                             'font_style': 'oblique'})
                          )

    t.add("", data, symbol='arrow', orient='TB')
    t.render(output_file)
    print("Successfully generated")


