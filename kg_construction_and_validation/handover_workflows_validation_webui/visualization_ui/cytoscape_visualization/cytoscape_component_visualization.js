export default {
  template: `
    <div style="position: relative; width: 100%;  height: calc(100vh - 140px); min-height: 700px;">
      <div ref="cy" style="width: 100%; height: 100%; display: block; border: 1px solid #ddd; border-radius: 8px;"></div>
      <div v-if="hoverData" 
           :style="{ 
             position: 'absolute', 
             top: hoverPos.y + 'px', 
             left: hoverPos.x + 'px', 
             background: '#333', 
             color: '#fff', 
             padding: '8px', 
             borderRadius: '4px', 
             fontSize: '12px', 
             zIndex: 10, 
             pointerEvents: 'none',
             transform: 'translate(-50%, -120%)',
             whiteSpace: 'pre-wrap'
           }">
        <strong>Validation result:</strong> {{ hoverData }}
      </div>
    </div>
  `,

  props: {
    nodes: Array,
    edges: Array,
  },

  data() {
    return {
      hoverData: null,
      hoverPos: { x: 0, y: 0 }
    };
  },

  mounted() {
    this.cy = cytoscape({
      container: this.$refs.cy,

      elements: {
        nodes: this.nodes,
        edges: this.edges,
      },

      style: [
        // Default node style
        {
          selector: 'node',
          style: {
            'label': (ele) => {
              const label = ele.data('label') || '';
              const activities = ele.data('activities') || [];

              if (activities.length === 0) {
                return label;
              }

              const activityText = '\n'+activities.join('\n');

              const projects = ele.data('projects') || "";
              if (projects.length === 0) return `${label}\n${activityText}`;
              if (projects.length === 1) return `${label}\n${activityText}\n\nProject ${projects[0]}`;

              return `${label}\n${activityText}\n\nProjects\n${projects.join(',\n')}`;
            },
            'background-color': (ele) => this.getNodeBackgroundColor(ele),            'color': '#000000',
            'text-wrap': 'wrap',
            'text-max-width': '120px',
            'text-valign': 'bottom',
            'text-margin-y': '5px',
            'text-halign': 'center',
            'font-size': '12px',

            'padding': '10px',
            'text-wrap': 'wrap',
          }
        },
        {
          selector: 'node.step',
          style: {
          }
        },

        {
          selector: 'node.object',
          style: {
            'shape': 'round-rectangle',
          }
        },

        {
          selector: 'node.invisible',
          style: {
            'width': 0,
            'height': 0,
            'padding': 0,
            'background-opacity': 0,
            'border-width': 0,
            'label': '',
            'text-opacity': 0,
          }
        },

        // Selected nodes (applied on top of step/object styles)
        {
          selector: '.selected',
          style: {
            'border-width': 2,
            'border-color': '#5898d4',
            'font-weight': 'bold',
          }
        },

        // Node coloring according to validation results
        {
          selector: '.valid_step',
          style: {
            'background-color': '#369c4e',
            'background-image': [
              '/assets/check_circle.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        {
          selector: '.invalid_step',
          style: {
            'background-color': '#d40820',
            'background-image': [
              '/assets/error.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        { // The step had missing handovers
          selector: '.missing_step',
          style: {
            'background-color': '#e88b00',
            'background-image': [
              '/assets/error.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        { // Any and all steps after a "missing handovers" step
          selector: '.not_checked_step',
          style: {
            'background-color': '#454549',
            'background-image': [
              '/assets/error.svg',
            ],
            'background-fit': 'contain',
            'background-image-opacity': 0.5,
            'background-image-containment': 'inside'
          }
        },
        {
          selector: 'edge',
          style: {
            'width': 2,
            'line-color': '#37648f',
            'curve-style': 'round-taxi',
            'target-arrow-shape': 'triangle',
            'target-arrow-color': '#37648f',
            'source-arrow-color': '#37648f',
            'arrow-scale': 1.2,
          }
        },
        {
          selector: 'edge.sequence-edge',
          style: {
            'curve-style': 'straight',
            'target-arrow-shape': 'triangle',
            'target-arrow-color': '#37648f',
            'line-color': '#37648f'
          }
        },
        {
          selector: 'edge.branch-edge',
          style: {
            'curve-style': 'taxi',
            'taxi-direction': 'downward',
            'taxi-turn': '50%',
            'target-arrow-shape': 'triangle',
            'target-arrow-color': '#37648f',
            'line-color': '#37648f'
          }
        }
      ],
      layout: {
        name: 'preset'
      }
    });

    // Tooltip listeners
    const validationSelector = 'node.valid_step, node.invalid_step, node.missing_step, node.not_checked_step';

    this.cy.on('mouseover', validationSelector, (evt) => {
      const node = evt.target;
      const msg = node.data('validationTooltip');
      if (msg) {
        this.hoverData = msg;
        this.updateTooltipPos(evt);
      }
    });

    this.cy.on('mousemove', validationSelector, (evt) => {
      if (this.hoverData) this.updateTooltipPos(evt);
    });

    this.cy.on('mouseout', validationSelector, () => {
      this.hoverData = null;
    });

    // Selection listeners
    this.cy.on('tap', 'node', (evt) => {
      const node = evt.target;
      this.cy.elements().removeClass('selected');
      node.addClass('selected');
      this.$emit('nodeClick', {
        id: node.id(),
        label: node.data('label'),
        kind: node.data('kind'),
        identifiers_for_coloring: node.data('identifiers_for_coloring') || []
      });
    });

    this.cy.on('tap', (evt) => {
      if (evt.target === this.cy) this.cy.elements().removeClass('selected');
    });

    this.$nextTick(() => {
      requestAnimationFrame(() => {
        this.rerun_layout_and_fit();
      });
    });
  },

  methods: {
    nodeHasKind(node, kind) {
      const rawIds = node.data('identifiers_for_coloring') || [];
      const ids = Array.isArray(rawIds) ? rawIds : [rawIds];

      return (
        node.data('kind') === kind ||
        ids.includes(kind) ||
        node.hasClass(kind)
      );
    },

    getNodeBackgroundColor(node) {
      if (this.nodeHasKind(node, 'sample')) return '#efb0f2';
      if (this.nodeHasKind(node, 'ho_g')) return '#55c990';
      if (this.nodeHasKind(node, 'ho_o_g')) return '#ff8f85';
      if (this.nodeHasKind(node, 'act')) return '#f4b1c4';
      if (this.nodeHasKind(node, 'output')) return '#55c7ef';

      return node.data('color') || '#0074D9';
    },    
    updateTooltipPos(evt) {
      const pos = evt.renderedPosition;
      this.hoverPos = { x: pos.x, y: pos.y };
    },

    rerun_layout_and_fit() {
      this.applyWorkflowGridLayout({
        xGap: 180,
        yGap: 200,
        branchGap: 50,
        padding: 80
      });
    },

    applyWorkflowGridLayout(options = {}) {
      const xGap = options.xGap ?? 180;
      const yGap = options.yGap ?? 200;
      const branchGap = options.branchGap ?? 50;
      const padding = options.padding ?? 80;

      const minNodeWidth = options.minNodeWidth ?? 90;
      const minColumnWidth = options.minColumnWidth ?? xGap;

      const isHg = (node) => this.nodeHasKind(node, 'ho_g');

      const estimateNodeWidth = (node) => {
        const label = node.data('label') || '';

        // Rough label-width estimate.
        // Increase 7 or minNodeWidth if labels still overlap.
        return Math.min(
          Math.max(label.length * 7, minNodeWidth),
          420
        );
      };

      const getNonSequenceChildren = (node) => {
        return node.outgoers('edge')
          .map(edge => edge.target())
          .filter(target => {
            // HG -> HG is the main horizontal workflow sequence.
            // Everything else goes downward recursively.
            return !(isHg(node) && isHg(target));
          });
      };

      // Mark edge types for styling.
      this.cy.edges().forEach(edge => {
        edge.removeClass('sequence-edge');
        edge.removeClass('branch-edge');

        const sourceIsHg = isHg(edge.source());
        const targetIsHg = isHg(edge.target());

        if (sourceIsHg && targetIsHg) {
          edge.addClass('sequence-edge');
        } else {
          edge.addClass('branch-edge');
        }
      });

      const hgNodes = this.cy.nodes().filter(node => isHg(node));

      if (hgNodes.length === 0) {
        this.cy.resize();
        this.cy.fit(this.cy.elements(), padding);
        return;
      }

      // HG -> HG edges define the main horizontal chain.
      const hgEdges = this.cy.edges().filter(edge => {
        return isHg(edge.source()) && isHg(edge.target());
      });

      const hgTargets = new Set(hgEdges.map(edge => edge.target().id()));

      const hgNodeArray = hgNodes.toArray();

      const labelOf = (node) => {
        return (node.data('label') || node.id() || '').toLowerCase();
      };

      const rootSortScore = (node) => {
        const label = labelOf(node);

        // Prefer the actual initial-work chain as the first horizontal chain.
        if (label.includes('initial_work')) return 0;
        if (label.includes('_initial_')) return 0;

        return 1;
      };

      const compareHgNodes = (a, b) => {
        const scoreDiff = rootSortScore(a) - rootSortScore(b);

        if (scoreDiff !== 0) {
          return scoreDiff;
        }

        return labelOf(a).localeCompare(labelOf(b));
      };

      const getHgSuccessors = (node) => {
        return node.outgoers('edge')
          .map(edge => edge.target())
          .filter(target => isHg(target))
          .sort(compareHgNodes);
      };

      // Roots = HG nodes that are not target of another HG.
      let rootHgNodes = hgNodeArray
        .filter(node => !hgTargets.has(node.id()))
        .sort(compareHgNodes);

      // Fallback for cycles or weird data.
      if (rootHgNodes.length === 0 && hgNodeArray.length > 0) {
        rootHgNodes = [hgNodeArray.sort(compareHgNodes)[0]];
      }

      const orderedHg = [];
      const visitedHg = new Set();

      rootHgNodes.forEach(root => {
        let current = root;

        while (current && !visitedHg.has(current.id())) {
          orderedHg.push(current);
          visitedHg.add(current.id());

          const nextHg = getHgSuccessors(current)
            .find(node => !visitedHg.has(node.id()));

          current = nextHg || null;
        }
      });

      // Add disconnected or missed HG nodes at the end.
      hgNodeArray
        .sort(compareHgNodes)
        .forEach(node => {
          if (!visitedHg.has(node.id())) {
            orderedHg.push(node);
            visitedHg.add(node.id());
          }
        });

      const subtreeWidthCache = new Map();

      const calculateSubtreeWidth = (node, path = new Set()) => {
        const nodeId = node.id();

        // Cycle protection.
        if (path.has(nodeId)) {
          return estimateNodeWidth(node);
        }

        if (subtreeWidthCache.has(nodeId)) {
          return subtreeWidthCache.get(nodeId);
        }

        const nextPath = new Set(path);
        nextPath.add(nodeId);

        const children = getNonSequenceChildren(node);

        if (children.length === 0) {
          const width = estimateNodeWidth(node);
          subtreeWidthCache.set(nodeId, width);
          return width;
        }

        const childrenWidth = children.reduce((sum, child, index) => {
          const childWidth = calculateSubtreeWidth(child, nextPath);
          const gap = index === 0 ? 0 : branchGap;
          return sum + gap + childWidth;
        }, 0);

        const width = Math.max(
          estimateNodeWidth(node),
          childrenWidth
        );

        subtreeWidthCache.set(nodeId, width);
        return width;
      };

      const positioned = new Set();

      const positionSubtree = (node, centerX, level, path = new Set()) => {
        const nodeId = node.id();

        // Cycle protection.
        if (path.has(nodeId)) {
          return;
        }

        const nextPath = new Set(path);
        nextPath.add(nodeId);

        // If a node is reached twice, keep the first position.
        // This avoids jumping in graphs with shared descendants.
        if (!positioned.has(nodeId)) {
          node.position({
            x: centerX,
            y: level * yGap
          });

          positioned.add(nodeId);
        }

        const children = getNonSequenceChildren(node);

        if (children.length === 0) {
          return;
        }

        const childWidths = children.map(child => {
          return calculateSubtreeWidth(child, nextPath);
        });

        const totalChildrenWidth = childWidths.reduce((sum, width, index) => {
          const gap = index === 0 ? 0 : branchGap;
          return sum + gap + width;
        }, 0);

        let currentX = centerX - totalChildrenWidth / 2;

        children.forEach((child, index) => {
          const childWidth = childWidths[index];
          const childCenterX = currentX + childWidth / 2;

          positionSubtree(child, childCenterX, level + 1, nextPath);

          currentX += childWidth + branchGap;
        });
      };

      // Calculate width needed for each HG column.
      const columnWidths = orderedHg.map(hg => {
        const branchChildren = getNonSequenceChildren(hg);

        if (branchChildren.length === 0) {
          return Math.max(minColumnWidth, estimateNodeWidth(hg));
        }

        const branchWidths = branchChildren.map(child => {
          return calculateSubtreeWidth(child);
        });

        const totalBranchWidth = branchWidths.reduce((sum, width, index) => {
          const gap = index === 0 ? 0 : branchGap;
          return sum + gap + width;
        }, 0);

        return Math.max(
          minColumnWidth,
          estimateNodeWidth(hg),
          totalBranchWidth
        );
      });

      // Position HG columns horizontally.
      let currentColumnLeft = 0;

      orderedHg.forEach((hg, index) => {
        const columnWidth = columnWidths[index];
        const hgCenterX = currentColumnLeft + columnWidth / 2;

        // HG row.
        hg.position({
          x: hgCenterX,
          y: 0
        });

        positioned.add(hg.id());

        const branchChildren = getNonSequenceChildren(hg);

        if (branchChildren.length > 0) {
          const branchWidths = branchChildren.map(child => {
            return calculateSubtreeWidth(child);
          });

          const totalBranchWidth = branchWidths.reduce((sum, width, childIndex) => {
            const gap = childIndex === 0 ? 0 : branchGap;
            return sum + gap + width;
          }, 0);

          let branchLeft = hgCenterX - totalBranchWidth / 2;

          branchChildren.forEach((child, childIndex) => {
            const childWidth = branchWidths[childIndex];
            const childCenterX = branchLeft + childWidth / 2;

            // Children start at level 1.
            // Then recursion handles level 2, 3, 4, ...
            positionSubtree(child, childCenterX, 1);

            branchLeft += childWidth + branchGap;
          });
        }

        currentColumnLeft += columnWidth + xGap;
      });

      this.cy.resize();

      this.cy.layout({
        name: 'preset',
        fit: false,
        animate: false
      }).run();

      this.cy.fit(this.cy.elements(), padding);
    },

    addEdge(source, target) {
      this.cy.add({
        group: 'edges',
        data: { source: source, target: target }
      });

      this.rerun_layout_and_fit();
    },

    existsEdge(source, target) {
      return this.cy.edges(`[source = "${source}"][target = "${target}"]`).length > 0;
    },

    removeEdge(source, target) {
      const edge = this.cy.edges(`[source = "${source}"][target = "${target}"]`);

      if (edge.length > 0) {
        edge.remove();
        this.rerun_layout_and_fit();
      }
    },

    renameNode(id, newLabel) {
      // We only change the node's label to avoid recreating the whole
      // cytoscape data structure again
      const node = this.cy.$id(id);
      node.data('label', newLabel);

      // Force a re-render of the label
      node.trigger('data');
    },

    addNode(id, label, type, node_color) {
      if (this.cy.$id(id).length === 0) {
        let classes = [];
        if (type && ['step', 'object'].includes(type.toLowerCase())) {
          classes.push(type.toLowerCase());
        }

        this.cy.add({
          group: 'nodes',
          data: {
            id: id,
            label: label,
            activities: [],
            color: node_color
          },
          classes: classes
        });

        this.rerun_layout_and_fit();
      }
    },

    removeNode(node_id) {
      const node = this.cy.getElementById(node_id);

      if (node.length > 0) {
        node.remove();
        this.rerun_layout_and_fit();
      }
    },

    selectNode(id) {
      this.cy.elements().removeClass('selected');
      const node = this.cy.$id(id);
      if (node.length > 0) node.addClass('selected');
    },

    clearValidationResults() {
      this.cy.elements().removeClass('valid_step');
      this.cy.elements().removeClass('invalid_step');
      this.cy.elements().removeClass('missing_step');
      
      // Clear stored tooltips
      this.cy.nodes().removeData('validationTooltip');
    },

    setNodeAsValid(id, tooltip = 'Valid') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('valid_step');
        node.data('validationTooltip', tooltip);
      }
    },
    
    setNodeAsInvalid(id, tooltip = 'Invalid') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('invalid_step');
        node.data('validationTooltip', tooltip);
      }
    },
    
    setNodeAsMissing(id, tooltip = 'Missing') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('missing_step');
        node.data('validationTooltip', tooltip);
      }
    },

    setNodeAsNotChecked(id, tooltip = 'Not checked') {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.addClass('not_checked_step');
        node.data('validationTooltip', tooltip);
      }
    },

    addActivity(id, activity, new_node_color) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        const activities = node.data('activities') || [];
        if (!activities.includes(activity)) {
          node.data('activities', [...activities, activity]);
          node.data('color', new_node_color);
          node.trigger('data');
          this.rerun_layout_and_fit();
        }
      }
    },

    removeActivity(id, activity) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        const activities = node.data('activities') || [];
        const filtered = activities.filter(a => a !== activity);
        node.data('activities', filtered);
        node.trigger('data');
        this.rerun_layout_and_fit();
      }
    },

    replaceActivities(id, activities, new_node_color) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.data('activities', activities);

        // Force a re-render of the label and its color
        node.data('color', new_node_color);
        node.trigger('data');
        this.rerun_layout_and_fit();
      }
    },

    replaceProjects(id, projects) {
      const node = this.cy.$id(id);
      if (node.length > 0) {
        node.data('projects', projects);

        // Force a re-render of the label and its color
        node.trigger('data');
        this.rerun_layout_and_fit();
      }
    }
  }
};