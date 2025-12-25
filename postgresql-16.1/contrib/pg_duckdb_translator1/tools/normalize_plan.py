#!/usr/bin/env python3
import re
import argparse
from pathlib import Path

def normalize_lines(lines):
    out = []
    for ln in lines:
        s = ln.rstrip('\n')
        # remove box-drawing chars
        s = re.sub(r'[┌┐└┘│─+]+', '', s)
        s = s.strip()
        if not s:
            continue
        # drop ~0 rows lines or lines that only contain rows counts
        if re.search(r'~?\d+\s+rows', s):
            continue
        # remove table qualifiers like memory.main.lineitem   .
        s = re.sub(r'\b[a-zA-Z0-9_\.]+\s*\.[\s]*', '', s)
        # unify internal compress/decompress names
        s = s.replace('__internal_compress_string_', 'COMPRESS')
        s = s.replace('__internal_decompress_string_', 'DECOMPRESS')
        # normalize g_(colN) and g(#N) -> GCOL
        s = re.sub(r'g_\(col\d+\)', 'GCOL', s)
        s = re.sub(r'g\(#\d+\)', 'GCOL', s)
        # unify #N -> COL
        s = re.sub(r'#\d+', 'COL', s)
        # remove type wrappers like utinyint(...)  -> unwrap to inner
        s = re.sub(r'\butinyint\(([^)]+)\)', r"\1", s)
        s = re.sub(r'\bint8\(([^)]+)\)', r"\1", s)
        # remove parentheses around simple single identifiers (e.g., (l_returnflag) -> l_returnflag)
        s = re.sub(r'\((\w+)\)', r"\1", s)
        # normalize dates like 1998-09-02 -> <DATE>
        s = re.sub(r'\d{4}-\d{2}-\d{2}', '<DATE>', s)
        # normalize floats like 1998.0 -> 1998
        s = re.sub(r'\b(\d+)\.0\b', r"\1", s)
        # collapse multiple spaces
        s = re.sub(r"\s+", ' ', s).strip()
        out.append(s)
    return out


def main():
    p = argparse.ArgumentParser(description='Normalize planner ASCII plans and diff them')
    p.add_argument('pgsql', nargs='?', default='/tmp/q1_plan_pgsql_after.txt', help='Postgres-translated plan file')
    p.add_argument('duckdb', nargs='?', default='/tmp/q1_plan_duckdb_after.txt', help='DuckDB planner plan file')
    p.add_argument('--out-prefix', default='/tmp/q1_plan', help='Output prefix for normalized files')
    p.add_argument('--semantic', action='store_true', help='Run semantic/node-level comparison')
    p.add_argument('--structured', action='store_true', help='Run structured/tree-level comparison')
    args = p.parse_args()

    f_pg = Path(args.pgsql)
    f_dd = Path(args.duckdb)
    if not f_pg.exists() or not f_dd.exists():
        print('Error: one or both input files do not exist: ', f_pg, f_dd)
        return 2

    pg_lines = f_pg.read_text(encoding='utf-8', errors='ignore').splitlines()
    dd_lines = f_dd.read_text(encoding='utf-8', errors='ignore').splitlines()

    pg_norm = normalize_lines(pg_lines)
    dd_norm = normalize_lines(dd_lines)

    out_pg = Path(args.out_prefix + '_pgsql_norm.txt')
    out_dd = Path(args.out_prefix + '_duckdb_norm.txt')
    out_diff = Path(args.out_prefix + '_norm_diff.txt')

    out_pg.write_text('\n'.join(pg_norm) + '\n')
    out_dd.write_text('\n'.join(dd_norm) + '\n')

    # produce a unified diff using difflib
    import difflib
    diff = list(difflib.unified_diff(pg_norm, dd_norm, fromfile=str(out_pg), tofile=str(out_dd), lineterm=''))
    out_diff.write_text('\n'.join(diff) + '\n')

    print('Wrote normalized files:')
    print(' -', out_pg)
    print(' -', out_dd)
    print('Unified diff ->', out_diff)
    print('\nSummary:')
    print(' - pg lines (orig):', len(pg_lines), 'norm:', len(pg_norm))
    print(' - dd lines (orig):', len(dd_lines), 'norm:', len(dd_norm))
    print(' - diff lines:', len(diff))
    if diff:
        print('\nTop diff lines (first 40):')
        for i, l in enumerate(diff[:40]):
            print(l)

    # If user requested semantic comparison, parse normalized lines into nodes
    if args.semantic:
        semantic_out = Path(args.out_prefix + '_semantic_diff.txt')
        sem_report = semantic_compare_fuzzy(pg_norm, dd_norm)
        semantic_out.write_text(sem_report + '\n')
        print('\nWrote semantic diff ->', semantic_out)

    if args.structured:
        struct_out = Path(args.out_prefix + '_structured_diff.txt')
        struct_report = structured_compare(pg_norm, dd_norm)
        struct_out.write_text(struct_report + '\n')
        print('\nWrote structured diff ->', struct_out)

    return 0


def parse_nodes(norm_lines):
    """Parse normalized lines into a sequence of nodes. Each node is a dict with 'type' and 'content' list."""
    nodes = []
    known = set(['PROJECTION', 'ORDER_BY', 'AGGREGATE', 'FILTER', 'SEQ_SCAN'])
    i = 0
    while i < len(norm_lines):
        l = norm_lines[i]
        if l in known:
            node_type = l
            i += 1
            content = []
            # gather until next known node or end
            while i < len(norm_lines) and norm_lines[i] not in known:
                row = norm_lines[i]
                # skip structural markers
                if row in ('Expressions:', 'Groups:', 'File Filters:', 'Filters:'):
                    i += 1
                    continue
                if row in ('┬', '┴'):
                    i += 1
                    continue
                content.append(row)
                i += 1
            # normalize content as set for fuzzy matching
            content_set = set([c for c in content if c])
            nodes.append({'type': node_type, 'content': content, 'set': content_set})
        else:
            i += 1
    return nodes


def jaccard(a, b):
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    inter = len(a & b)
    uni = len(a | b)
    return inter / uni if uni else 0.0


def semantic_compare(pg_norm, dd_norm):
    """Compare two normalized plan line lists semantically (node-level). Returns a text report."""
    pg_nodes = parse_nodes(pg_norm)
    dd_nodes = parse_nodes(dd_norm)
    report_lines = []
    report_lines.append(f'PG nodes: {len(pg_nodes)}, DD nodes: {len(dd_nodes)}')

    # compare in-order with fuzzy matching
    matched = 0
    unmatched = []
    N = max(len(pg_nodes), len(dd_nodes))
    for i in range(N):
        pg = pg_nodes[i] if i < len(pg_nodes) else None
        dd = dd_nodes[i] if i < len(dd_nodes) else None
        if pg and dd:
            same_type = (pg['type'] == dd['type'])
            sim = jaccard(pg['set'], dd['set'])
            if same_type and sim >= 0.5:
                matched += 1
                report_lines.append(f'Node {i}: MATCH type={pg["type"]} sim={sim:.2f}')
            else:
                unmatched.append((i, pg, dd, sim))
                report_lines.append(f'Node {i}: DIFF type_pg={pg["type"]} type_dd={dd["type"]} sim={sim:.2f}')
        elif pg and not dd:
            unmatched.append((i, pg, None, 0.0))
            report_lines.append(f'Node {i}: PG only type={pg["type"]}')
        elif dd and not pg:
            unmatched.append((i, None, dd, 0.0))
            report_lines.append(f'Node {i}: DD only type={dd["type"]}')

    report_lines.append(f'\nMatched nodes: {matched} / {N}')
    report_lines.append(f'Unmatched node count: {len(unmatched)}')
    if unmatched:
        report_lines.append('\nDetails of first 10 mismatches:')
        for tup in unmatched[:10]:
            idx, pg, dd, sim = tup
            report_lines.append(f'--- Mismatch at node index {idx} (sim={sim:.2f}) ---')
            if pg:
                report_lines.append('PG node type: ' + pg['type'])
                report_lines.append('PG content:')
                for ln in pg['content'][:20]:
                    report_lines.append('  ' + ln)
            else:
                report_lines.append('PG node: <missing>')
            if dd:
                report_lines.append('DD node type: ' + dd['type'])
                report_lines.append('DD content:')
                for ln in dd['content'][:20]:
                    report_lines.append('  ' + ln)
            else:
                report_lines.append('DD node: <missing>')

    return '\n'.join(report_lines)


def semantic_compare_fuzzy(pg_norm, dd_norm):
    """Improved semantic comparison using greedy bipartite matching by Jaccard similarity."""
    pg_nodes = parse_nodes(pg_norm)
    dd_nodes = parse_nodes(dd_norm)
    report_lines = []
    report_lines.append(f'PG nodes: {len(pg_nodes)}, DD nodes: {len(dd_nodes)}')

    # compute similarity matrix
    sims = {}
    for i, pg in enumerate(pg_nodes):
        for j, dd in enumerate(dd_nodes):
            sim = jaccard(pg['set'], dd['set'])
            sims[(i, j)] = sim

    matched_pg = set()
    matched_dd = set()
    matches = []

    # greedy match by highest similarity
    for _ in range(min(len(pg_nodes), len(dd_nodes))):
        # find best unmatched pair
        best = None
        best_sim = 0.0
        for (i, j), sim in sims.items():
            if i in matched_pg or j in matched_dd:
                continue
            if sim > best_sim:
                best_sim = sim
                best = (i, j, sim)
        if not best or best_sim <= 0.0:
            break
        i, j, sim = best
        matched_pg.add(i)
        matched_dd.add(j)
        matches.append((i, j, sim))

    # report matches
    matched = len(matches)
    report_lines.append(f'Matched pairs: {matched} (greedy best matches)')
    avg_sim = (sum(m[2] for m in matches) / matched) if matched else 0.0
    report_lines.append(f'Average similarity among matches: {avg_sim:.2f}')
    report_lines.append('')

    for (i, j, sim) in matches:
        pg = pg_nodes[i]
        dd = dd_nodes[j]
        type_ok = (pg['type'] == dd['type'])
        report_lines.append(f'Pair PG#{i}({pg["type"]}) <-> DD#{j}({dd["type"]}) sim={sim:.2f} type_match={type_ok}')

    # unmatched
    unmatched_pg = [i for i in range(len(pg_nodes)) if i not in matched_pg]
    unmatched_dd = [j for j in range(len(dd_nodes)) if j not in matched_dd]
    report_lines.append('')
    report_lines.append(f'Unmatched PG nodes: {len(unmatched_pg)}')
    report_lines.append(f'Unmatched DD nodes: {len(unmatched_dd)}')

    if unmatched_pg:
        report_lines.append('\nFirst unmatched PG nodes (up to 5):')
        for i in unmatched_pg[:5]:
            report_lines.append(f'PG#{i} type={pg_nodes[i]["type"]}')
            for ln in pg_nodes[i]['content'][:10]:
                report_lines.append('  ' + ln)

    if unmatched_dd:
        report_lines.append('\nFirst unmatched DD nodes (up to 5):')
        for j in unmatched_dd[:5]:
            report_lines.append(f'DD#{j} type={dd_nodes[j]["type"]}')
            for ln in dd_nodes[j]['content'][:10]:
                report_lines.append('  ' + ln)

    return '\n'.join(report_lines)


def parse_structured_node_content(content_lines):
    """Parse a node's content lines into structured fields.
    Returns dict with keys depending on node type: expressions, groups, filters, table_info.
    """
    node = {'expressions': [], 'groups': [], 'filters': [], 'table': None, 'other': []}
    i = 0
    while i < len(content_lines):
        l = content_lines[i]
        # detect groups
        if l.startswith('Groups:'):
            i += 1
            while i < len(content_lines) and content_lines[i] and not content_lines[i].endswith(':'):
                node['groups'].append(content_lines[i])
                i += 1
            continue
        # detect expressions
        if l == 'Expressions:' or l.startswith('Expressions'):
            i += 1
            while i < len(content_lines) and content_lines[i] and not content_lines[i].endswith(':'):
                node['expressions'].append(content_lines[i])
                i += 1
            continue
        # detect filters or file filters
        if l.startswith('Filters:') or l.startswith('File Filters:'):
            # consume remaining lines as filter block
            i += 1
            while i < len(content_lines) and content_lines[i]:
                node['filters'].append(content_lines[i])
                i += 1
            continue
        # detect table info lines
        if l.startswith('Table:') or l.startswith('Type:'):
            if node['table'] is None:
                node['table'] = []
            node['table'].append(l)
            i += 1
            continue
        # otherwise other
        node['other'].append(l)
        i += 1
    return node


def structured_compare(pg_norm, dd_norm):
    """Do a structured/tree-level comparison and return a multi-line report string."""
    pg_nodes = parse_nodes(pg_norm)
    dd_nodes = parse_nodes(dd_norm)
    report = []
    report.append(f'PG nodes: {len(pg_nodes)}, DD nodes: {len(dd_nodes)}')

    # Build structured nodes
    pg_struct = [ {'type': n['type'], **parse_structured_node_content(n['content'])} for n in pg_nodes ]
    dd_struct = [ {'type': n['type'], **parse_structured_node_content(n['content'])} for n in dd_nodes ]

    # Compare sequence of nodes with flexible alignment: allow some insertions (e.g., extra PROJECTIONs)
    i = 0
    j = 0
    matches = []
    while i < len(pg_struct) and j < len(dd_struct):
        a = pg_struct[i]
        b = dd_struct[j]
        # exact type match or allow PROJECTION vs PROJECTION grouping
        type_match = (a['type'] == b['type'])
        # Heuristic: if one side has an extra PROJECTION that simply wraps the
        # next node on that side, allow skipping that PROJECTION and match the
        # inner node to the other side. This handles cases like
        # PG: PROJECTION -> ORDER_BY  vs DD: ORDER_BY -> PROJECTION and vice versa.
        # Check PG-side extra PROJECTION
        if a['type'] == 'PROJECTION' and i+1 < len(pg_struct) and pg_struct[i+1]['type'] == b['type']:
            # compare inner node with b
            a_next = pg_struct[i+1]
            expr_sim_next = jaccard(set(a_next['expressions']), set(b['expressions']))
            grp_sim_next = jaccard(set(a_next['groups']), set(b['groups']))
            filt_sim_next = jaccard(set(a_next['filters']), set(b['filters']))
            score_next = (0.5 * expr_sim_next) + (0.3 * grp_sim_next) + (0.2 * filt_sim_next)
            if score_next >= 0.4 or a_next['type'] == b['type']:
                matches.append((i, None, 0.0, 'pg-extra-projection'))
                matches.append((i+1, j, score_next, 'match'))
                i += 2
                j += 1
                continue
        # Check DD-side extra PROJECTION
        if b['type'] == 'PROJECTION' and j+1 < len(dd_struct) and dd_struct[j+1]['type'] == a['type']:
            b_next = dd_struct[j+1]
            expr_sim_next = jaccard(set(a['expressions']), set(b_next['expressions']))
            grp_sim_next = jaccard(set(a['groups']), set(b_next['groups']))
            filt_sim_next = jaccard(set(a['filters']), set(b_next['filters']))
            score_next = (0.5 * expr_sim_next) + (0.3 * grp_sim_next) + (0.2 * filt_sim_next)
            if score_next >= 0.4 or a['type'] == b_next['type']:
                matches.append((None, j, 0.0, 'dd-extra-projection'))
                matches.append((i, j+1, score_next, 'match'))
                i += 1
                j += 2
                continue
        expr_sim = jaccard(set(a['expressions']), set(b['expressions']))
        grp_sim = jaccard(set(a['groups']), set(b['groups']))
        filt_sim = jaccard(set(a['filters']), set(b['filters']))
        # scoring heuristic
        score = (0.5 * expr_sim) + (0.3 * grp_sim) + (0.2 * filt_sim)
        if type_match and score >= 0.4:
            matches.append((i, j, score, 'match'))
            i += 1
            j += 1
            continue
        # allow if b is PROJECTION and seems to wrap a
        if b['type'] == 'PROJECTION' and score >= 0.3:
            matches.append((i, j, score, 'proj-wrap'))
            i += 1
            j += 1
            continue
        # if b is PROJECTION but low sim, maybe extra projection: advance j
        if b['type'] == 'PROJECTION':
            matches.append((None, j, 0.0, 'dd-extra-projection'))
            j += 1
            continue
        # otherwise advance i
        matches.append((i, None, 0.0, 'pg-only'))
        i += 1

    # record leftovers
    while i < len(pg_struct):
        matches.append((i, None, 0.0, 'pg-only'))
        i += 1
    while j < len(dd_struct):
        matches.append((None, j, 0.0, 'dd-only'))
        j += 1

    # build report
    matched_count = sum(1 for m in matches if m[3] in ('match','proj-wrap'))
    report.append(f'Matched (approx): {matched_count} / {max(len(pg_struct), len(dd_struct))}')
    report.append('Details:')
    for m in matches:
        pi, dj, sc, kind = m
        if kind == 'match':
            report.append(f'PG#{pi}({pg_struct[pi]["type"]}) <-> DD#{dj}({dd_struct[dj]["type"]}) score={sc:.2f}')
        elif kind == 'proj-wrap':
            report.append(f'PG#{pi}({pg_struct[pi]["type"]}) wrapped-by PROJECTION DD#{dj} score={sc:.2f}')
        elif kind == 'dd-extra-projection':
            report.append(f'DD extra PROJECTION at DD#{dj}')
        elif kind == 'pg-only':
            report.append(f'PG only node PG#{pi} type={pg_struct[pi]["type"]}')
        elif kind == 'dd-only':
            report.append(f'DD only node DD#{dj} type={dd_struct[dj]["type"]}')

    # show first few unmatched details
    report.append('\nFirst unmatched details (up to 6):')
    cnt = 0
    for m in matches:
        if m[3] in ('pg-only','dd-only') and cnt < 6:
            pi, dj, sc, kind = m
            report.append('---')
            if kind == 'pg-only':
                n = pg_struct[pi]
                report.append(f'PG#{pi} type={n["type"]}')
                report.append('Expressions:')
                report.extend('  '+x for x in n['expressions'][:10])
                report.append('Filters:')
                report.extend('  '+x for x in n['filters'][:10])
            else:
                n = dd_struct[dj]
                report.append(f'DD#{dj} type={n["type"]}')
                report.append('Expressions:')
                report.extend('  '+x for x in n['expressions'][:10])
                report.append('Filters:')
                report.extend('  '+x for x in n['filters'][:10])
            cnt += 1

    return '\n'.join(report)

if __name__ == '__main__':
    raise SystemExit(main())
