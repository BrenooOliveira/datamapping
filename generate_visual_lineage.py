#!/usr/bin/env python3
"""
generate_lineage_pyvis.py

Gera um grafo interativo HTML (pyvis) de data lineage a partir de um CSV de mapeamento.

CSV esperado (colunas): "Tabela Final", "DataFrame", "Origem", "Transformação"
- Origem pode ser uma lista separada por vírgula (ex: "clientes_df, enderecos_df" ou "data/clientes.csv")
"""

import pandas as pd
import networkx as nx
from pyvis.network import Network
from pathlib import Path
import argparse

def split_origins(orig):
    if pd.isna(orig):
        return []
    parts = [p.strip() for p in str(orig).split(",") if p.strip() and p.strip() != "—"]
    return parts

def node_type_heuristic(name, final_table):
    # heurística simples para categorizar nós (para colorir/formatar)
    name = str(name)
    if name == final_table:
        return "final_table"
    if name.endswith("_df") or name.startswith("df_") or "fato" in name or "vw_" in name:
        return "dataframe"
    if "/" in name or name.endswith(".csv") or name.endswith(".parquet") or name.startswith("data/"):
        return "file"
    if "." in name:  # schema.table
        return "table"
    return "dataframe"

def build_graph_from_csv(csv_path):
    df = pd.read_csv(csv_path)

    # Normaliza nomes de colunas se necessário (aceita maiúsc/minúsc)
    cols_map = {}
    for col in df.columns:
        lc = col.strip().lower()
        if lc == "tabela final" or lc == "tabela_final" or lc == "final_table":
            cols_map[col] = "Tabela Final"
        elif lc == "dataframe":
            cols_map[col] = "DataFrame"
        elif lc == "origem":
            cols_map[col] = "Origem"
        elif lc == "transformação" or lc == "transformacao" or lc == "transform":
            cols_map[col] = "Transformação"
    if cols_map:
        df = df.rename(columns=cols_map)

    # garante colunas mínimas
    for c in ["Tabela Final", "DataFrame", "Origem", "Transformação"]:
        if c not in df.columns:
            df[c] = ""

    G = nx.DiGraph()

    for _, row in df.iterrows():
        final_table = str(row.get("Tabela Final", "")).strip() or None
        df_name = str(row.get("DataFrame", "")).strip() or None
        transform = str(row.get("Transformação", "")).strip()
        origem = row.get("Origem", "")
        origins = split_origins(origem)

        # adiciona nós e arestas origem -> df_name
        for origin in origins:
            if origin not in G:
                G.add_node(origin, type=node_type_heuristic(origin, final_table))
            edge_label = transform if transform and transform != "—" else ""
            G.add_edge(origin, df_name or origin, label=edge_label)

        # adiciona nó do dataframe
        if df_name and df_name not in G:
            G.add_node(df_name, type=node_type_heuristic(df_name, final_table))

        # liga dataframe -> tabela final (se diferente)
        if final_table and df_name and final_table != df_name:
            if final_table not in G:
                G.add_node(final_table, type="final_table")
            G.add_edge(df_name, final_table, label=transform if transform and transform != "—" else "")

    return G

def render_pyvis(G, output_html, notebook_mode=False, height="800px", width="100%"):
    net = Network(directed=True, height=height, width=width, notebook=notebook_mode)
    net.force_atlas_2based()

    type_style = {
        "file": {"color": "#f4a261", "shape": "box"},
        "table": {"color": "#2a9d8f", "shape": "ellipse"},
        "dataframe": {"color": "#264653", "shape": "dot"},
        "final_table": {"color": "#e76f51", "shape": "diamond"}
    }

    for n, attrs in G.nodes(data=True):
        ntype = attrs.get("type", "dataframe")
        style = type_style.get(ntype, {"color": "#888", "shape": "dot"})
        in_deg = G.in_degree(n)
        out_deg = G.out_degree(n)
        title = f"<b>{n}</b><br>Type: {ntype}<br>In: {in_deg} | Out: {out_deg}"
        net.add_node(n, label=n, title=title, color=style["color"], shape=style["shape"])

    for u, v, attrs in G.edges(data=True):
        lbl = attrs.get("label", "")
        net.add_edge(u, v, title=lbl, label=lbl, arrows="to")

    # opções de física/layout
    net.set_options("""
    var options = {
      "physics": {"forceAtlas2Based": {"gravitationalConstant": -50, "springLength": 100}, "solver": "forceAtlas2Based"},
      "manipulation": { "enabled": false }
    }
    """)
    net.show(output_html)
    return output_html

def main():
    parser = argparse.ArgumentParser(description="Gera grafo de lineage interativo (pyvis) a partir de data_mapping.csv")
    parser.add_argument("--csv", "-c", default="data_mapping.csv", help="CSV de input (padrão: data_mapping.csv)")
    parser.add_argument("--out", "-o", default="lineage_graph.html", help="HTML de saída (padrão: lineage_graph.html)")
    args = parser.parse_args()

    csv_path = Path(args.csv)
    if not csv_path.exists():
        raise FileNotFoundError(f"CSV não encontrado: {csv_path}")

    print(f"Lendo {csv_path} ...")
    G = build_graph_from_csv(csv_path)
    print(f"Nós: {len(G.nodes())}, Arestas: {len(G.edges())}")

    out = Path(args.out).resolve()
    print(f"Gerando HTML em {out} ...")
    render_pyvis(G, str(out))
    print(f"Concluído. Abra {out} no navegador.")

if __name__ == "__main__":
    main()
