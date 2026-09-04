/* extends async.sql into dependency chain processing and directly interacts
 * with its structures.
 */
DO
$bootstrap$
BEGIN

BEGIN
  PERFORM 1 FROM async.client_control;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Please install async client library first';
END;

BEGIN
  PERFORM 1 FROM flow.arguments LIMIT 0;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Please install flow client library first';
END;

BEGIN
  PERFORM 1 FROM flow.flow_configuration LIMIT 0;
  RETURN;
EXCEPTION WHEN undefined_table THEN NULL;
END;


/* Configures processing chain */
CREATE TABLE flow.flow_configuration
(
  flow TEXT PRIMARY KEY,
  concurrency_group_routine TEXT 
    CHECK (concurrency_group_routine SIMILAR TO '[a-z]+[a-z_0-9]*.?[a-z_0-9]*')
);

/* 
 * organizes work in dependency terms against a thing as instanced in the flow 
 * itself. Can have work in itself (which runs first) as well as underlying 
 * concurrent.  work is complete when all underlying steps are complete.
 */
CREATE TABLE flow.node
(
  node TEXT PRIMARY KEY CHECK (node SIMILAR TO '[a-z]+[a-z_0-9]*.?[a-z_0-9]*'), 

  target TEXT REFERENCES async.target ON UPDATE CASCADE ON DELETE CASCADE,

  /* callback to invoke.  presumed to be node unless given differently 
   * for example, if many nodes hit the same target.
   */
  /* use this as callback */
  routine TEXT DEFAULT NULL,
  /* use this as callback if node (priority over 'routine') */
  node_routine TEXT DEFAULT NULL,
  /* use this as callback if step (priority over 'routine') */
  step_routine TEXT DEFAULT NULL,

  priority INT,

  /* if set true, any step failing will immedaitely cause this node to stop 
   * processing (and cancel any work if there is any )
   */
  all_steps_must_complete BOOL DEFAULT true,

  /* task will immediately resolve complete when scheduled.  Set to true when
   * all interesting processing is within the steps themselves.  It's tempting
   * to assume this if there are steps, but frequently the node may itself push
   * steps dynamically.
   */
  empty BOOL DEFAULT false,

  /* If not null, each step will create a flow based on its own arguments.
   * There may not be any configured steps in that case.
   */
  steps_to_flow TEXT,

  /* overrides target asynchronous flag.  Set true when steps are configured
   * in the node locally.  If the node target is synchronous, this does nothing.
   */
  synchronous BOOL DEFAULT FALSE,

  /* nodes and steps can be configured to have a maximum run time.  If not set,
   * the target timeout will be checked and finally the async.control timneout
   * is used to establish maximum run time.
   */
  node_timeout INTERVAL,

  step_timeout INTERVAL

);

CREATE INDEX ON flow.node(target);

/* ataches a node to a flow. */
CREATE TABLE flow.flow_node
(
  flow TEXT REFERENCES flow.flow_configuration 
    ON UPDATE CASCADE ON DELETE CASCADE,
  node TEXT REFERENCES flow.node ON UPDATE CASCADE ON DELETE CASCADE,

  /* true if there are no paths from parent to any child that cross
   * through a continue_on_failure dependency, false if unknown or known to 
   * be false.  Can be computed from dependences, and is used to prevent 
   * creating 'DOA' tasks if they are not needed for dependency reasons.
   *
   * this is set up during configuration.
   */
  terminate_on_failure BOOL,  

  PRIMARY KEY (flow, node)
);

CREATE TABLE flow.dependency_configuration
(
  flow TEXT REFERENCES flow.flow_configuration ON DELETE CASCADE,
  parent TEXT REFERENCES flow.node(node) ON DELETE CASCADE,
  child TEXT REFERENCES flow.node(node) ON DELETE CASCADE,  

  continue_on_failure BOOL DEFAULT FALSE,

  PRIMARY KEY(flow, parent, child)
);

/* resident 1,2,3.. differentiated only by arguments and no external 
 * dependencies. 
 */
CREATE TABLE flow.step
(
  node TEXT REFERENCES flow.node ON UPDATE CASCADE ON DELETE CASCADE,
  arguments JSONB,

  PRIMARY KEY (node, arguments)
);

/* do not run these nodes concurrently
 *
 * XXX: not implemented
 */
CREATE TABLE flow.mutex
(
  left_node TEXT REFERENCES flow.node ON UPDATE CASCADE ON DELETE CASCADE,
  right_node TEXT REFERENCES flow.node ON UPDATE CASCADE ON DELETE CASCADE,
  PRIMARY KEY (left_node, right_node)
);

CREATE INDEX ON flow.mutex(right_node);

CREATE TABLE flow.flow
(
  flow_id BIGSERIAL PRIMARY KEY,
  flow TEXT NOT NULL REFERENCES flow.flow_configuration,
  arguments JSONB,
  created TIMESTAMPTZ DEFAULT now(),
  processed TIMESTAMPTZ,

  /* if created from a step, when the flow resolves, go back and mark that 
   * step complete as well.
   */
  parent_task_id BIGINT,

  /* if non-null, nodes not in this list will not be processed. */
  only_these_nodes TEXT[],

  /* if created from a step, parent_flow_id is here for purposes of fast
   * joining.
   */
  parent_flow_id BIGINT,

  /* cache a few variables in the flow itself when processed to speed lookups */
  count_nodes INT,

  count_finished_nodes INT,

  count_failed_nodes INT,

  first_error TEXT,

  /* tasks run at this priority. overrides node priority if not null */
  force_priority INT
  
);

CREATE INDEX ON flow.flow(flow);
CREATE INDEX ON flow.flow(parent_task_id);
CREATE INDEX ON flow.flow(flow_id) WHERE processed IS NULL;


CREATE TABLE flow.dependency
(
  flow_id INT REFERENCES flow.flow ON DELETE CASCADE,
  parent TEXT REFERENCES flow.node ON DELETE CASCADE,
  child TEXT REFERENCES flow.node ON DELETE CASCADE,

  continue_on_failure BOOL,

  PRIMARY KEY(flow_id, parent, child)
);

CREATE INDEX ON flow.dependency(child, flow_id);

CREATE TYPE flow.task_wrapper_t AS
(
  node TEXT,
  step_arguments JSONB
);



/* 
 * extend the task table so that the same exact step can not be implemented 
 * two or more times in the same flow.
 */
CREATE UNIQUE INDEX IF NOT EXISTS task_running_flow_idx ON async.task_running
  ( 
    (((task_data)->>'flow_id')::BIGINT),
    ((task_data)->>'node'),
    ((task_data)->'step_arguments')
  )
WHERE ((task_data)->>'flow_id')::BIGINT IS NOT NULL;  

CREATE UNIQUE INDEX IF NOT EXISTS task_complete_flow_idx ON async.task_complete
  ( 
    (((task_data)->>'flow_id')::BIGINT),
    ((task_data)->>'node'),
    ((task_data)->'step_arguments')
  )
WHERE ((task_data)->>'flow_id')::BIGINT IS NOT NULL;  

CREATE INDEX ON async.task_running(
  (((task_data)->>'flow_id')::BIGINT),
  ((task_data)->>'node')) 
WHERE 
  processed IS NULL
  AND ((task_data)->>'flow_id')::BIGINT IS NOT NULL;


END;
$bootstrap$;

DO
$code$
BEGIN

BEGIN
  PERFORM 1 FROM flow.flow_configuration LIMIT 0;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Flow server library incorrectly installed';
  RETURN;
END;

CREATE OR REPLACE FUNCTION flow.finish_parent() RETURNS TRIGGER AS
$$
DECLARE 
  s RECORD;
BEGIN
  IF old.processed IS NOT NULL
  THEN
    RETURN new;
  END IF;

  PERFORM async.finish(
    array[new.parent_task_id],
    'FINISHED'::async.finish_status_t, /* flows do not carry error state up to parent steps */
    format('via finishing of flow %s', new.flow_id))
  FROM async.task t
  WHERE 
    task_id = new.parent_task_id
    AND processed IS NULL;

  SELECT INTO s * FROM flow.v_flow_status_internal WHERE flow_id = new.flow_id;

  new.count_nodes = s.count_nodes;
  new.count_finished_nodes = s.count_finished_nodes;
  new.first_error = s.first_error;
  new.count_failed_nodes = s.count_failed_nodes;


  RETURN new;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE TRIGGER on_flow_update
  BEFORE INSERT OR UPDATE ON flow.flow
  FOR EACH ROW WHEN (
    new.processed IS NOT NULL
  )  
  EXECUTE PROCEDURE flow.finish_parent();


CREATE OR REPLACE FUNCTION flow.is_node(_step_arguments JSONB) RETURNS BOOL AS
$$
  SELECT _step_arguments = '{}'::JSONB;
$$ LANGUAGE SQL IMMUTABLE;


/* if there is a routine set in the flow to calculate concurrency group, do so */
CREATE OR REPLACE FUNCTION flow.concurrency_group(
  _flow_id BIGINT,
  _node TEXT,
  concurrency_group OUT TEXT) RETURNS TEXT AS
$$
DECLARE
  r RECORD;
  _routine TEXT;
BEGIN
  SELECT INTO r 
    concurrency_group_routine,
    arguments
  FROM flow.flow_configuration
  JOIN flow.flow f USING(flow)
  WHERE 
    flow_id = _flow_id
    AND concurrency_group_routine IS NOT NULL;

  IF NOT FOUND
  THEN
    RETURN;
  END IF;

  _routine := format(
    'SELECT %s(%s, %s)',
    quote_ident(r.concurrency_group_routine),
    _flow_id,
    quote_literal(_node));

  EXECUTE _routine INTO concurrency_group;
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE FUNCTION flow.configure_callback(
  _callback TEXT,
  _flow_id BIGINT,
  _flow TEXT,
  _node TEXT,
  _step_arguments JSONB,
  _steps_to_flow TEXT,
  _priority INT) RETURNS TEXT AS
$$
  SELECT 
    CASE WHEN _callback = '' THEN ''
    ELSE format(
      'CALL %s((%s, %s, flow.args(%s), %s, %s::JSONB, ##flow.TASK_ID##)::flow.callback_arguments_t)',
      _callback,
      _flow_id,
      quote_literal(_flow),
      _flow_id,
      quote_literal(_node),
      quote_literal(_step_arguments))
    END
  WHERE _steps_to_flow IS NULL OR flow.is_node(_step_arguments)
  UNION ALL SELECT format(
    'SELECT flow.create_flow(%s, %s::jsonb, _parent_task_id := ##flow.TASK_ID##, _force_priority := %s)',
    quote_literal(_steps_to_flow),
    quote_literal(_step_arguments),
    quote_nullable(_priority))
  WHERE NOT (_steps_to_flow IS NULL OR flow.is_node(_step_arguments))
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION flow.task_data(
  _flow_id BIGINT,
  _node TEXT,
  _step_arguments JSONB) RETURNS TEXT AS
$$
  SELECT jsonb_build_object(
    'flow_id', _flow_id,
    'node', _node,
    'step_arguments', _step_arguments);
$$ LANGUAGE SQL IMMUTABLE;


/* extends the async.task to have the json stored elements be produced as 
 * columns.  They are likely going to be indexed, so it's good to formalize
 * access to them.
 */ 
CREATE OR REPLACE VIEW flow.v_flow_task AS 
  SELECT 
    (task_data->>'flow_id')::BIGINT AS flow_id,
    task_data->>'node' AS node,
    task_data->'step_arguments' AS step_arguments,
    flow.is_node(task_data->'step_arguments') AS is_node,  
    t.*
  FROM async.task t
  /* important guard for partial index */
  WHERE (task_data->>'flow_id')::BIGINT IS NOT NULL;


/* Creates and sends tasks to async for processing.  It's tempting to locally 
 * record tasks for tracking purposes to simplify dependency lookups over the 
 * task table, but this introduces race conditions crossing the asynchronous
 * call boundary, for example task might compute before the local transaction
 * resolves.
 *
 * XXX: needs to trap error and fail flow if failed on unique constraint --
 * can happen if task cancels.
 */
CREATE OR REPLACE FUNCTION flow.push_tasks(
  _flow_id BIGINT,
  _tasks flow.task_wrapper_t[],
  _run_type async.task_run_type_t DEFAULT 'EXECUTE',
  _source TEXT DEFAULT NULL) RETURNS VOID AS 
$$
DECLARE
  r RECORD;
BEGIN
  PERFORM
    async.push_tasks(array_agg(
      (
        flow.task_data(
          _flow_id,
          t.node,
          t.step_arguments),
        n.target,
        COALESCE(f.force_priority, n.priority),
        flow.configure_callback(
          CASE WHEN flow.is_node(t.step_arguments) 
            THEN COALESCE(n.node_routine, n.routine, n.node)
            ELSE COALESCE(n.step_routine, n.routine, n.node)
          END,
          _flow_id,
          flow,
          node,
          step_arguments,
          n.steps_to_flow,
          COALESCE(f.force_priority, n.priority)),
        flow.concurrency_group(_flow_id, node),
        CASE WHEN flow.is_node(t.step_arguments) 
          THEN n.node_timeout
          ELSE n.step_timeout 
        END,
        /* by default, track yieled tasks for steps but not nodes */
        NOT flow.is_node(t.step_arguments)
      )::async.task_push_t),
    CASE 
      WHEN _run_type = 'EXECUTE' AND t.step_arguments = '{}' AND n.synchronous
        THEN 'EXECUTE_NOASYNC'
      ELSE _run_type
    END,
    _source)
  FROM flow.flow f
  CROSS JOIN 
  (
    SELECT  
      node,
      step_arguments
    FROM unnest(_tasks) t
    WHERE NOT EXISTS (
      SELECT 1 
      FROM flow.v_flow_task vt
      WHERE 
        vt.flow_id = _flow_id
        AND vt.node = t.node
        AND vt.step_arguments = t.step_arguments
    )
  ) t
  JOIN flow.node n USING(node)
  WHERE f.flow_id = _flow_id
  GROUP BY CASE 
      WHEN _run_type = 'EXECUTE' AND t.step_arguments = '{}' AND n.synchronous
        THEN 'EXECUTE_NOASYNC'
      ELSE _run_type
    END;

  FOR r IN 
    SELECT
      node,
      array_agg(arguments) args
    FROM unnest(_tasks) t
    JOIN flow.step USING(node)
    WHERE flow.is_node(step_arguments)
    GROUP BY 1
  LOOP
    CALL flow.push_steps(
      _flow_id,  
      r.node,
      r.args);
  END LOOP;
END;
$$ LANGUAGE PLPGSQL;






/* for a parent node, gives each child node and for those nodes if child
 * dependency requirements are met, so that child tasks can be queued.
 *
 * problem, node child eligible brfore steps complete
 */
CREATE OR REPLACE FUNCTION flow.node_child_dependencies(
  _flow_id BIGINT,
  _parent TEXT,
  child OUT TEXT,
  eligible OUT BOOL,
  run_type OUT async.task_run_type_t,
  ineligible_parents OUT TEXT[]) RETURNS SETOF RECORD AS
$$
  SELECT
    d.child,
    /* check both node and steps if any */
    count(*) FILTER (WHERE t.processed IS NULL) = 0,
    CASE 
      WHEN bool_or(t.failed AND NOT d2.continue_on_failure) THEN 'DOA'
      WHEN n.empty THEN 'EMPTY'
      WHEN f.only_these_nodes IS NULL THEN 'EXECUTE'
      WHEN d.child = ANY(f.only_these_nodes) THEN 'EXECUTE'
      ELSE 'EMPTY'
    END::async.task_run_type_t,  
    array_agg(d2.parent) FILTER (WHERE t.processed IS NULL)
  FROM flow.dependency d
  JOIN flow.flow f ON f.flow_id = _flow_id
  JOIN flow.node n ON d.child = n.node
  JOIN flow.dependency d2 ON 
    d2.flow_id = d.flow_id
    AND d2.child = d.child
  LEFT JOIN flow.v_flow_task t ON 
    t.flow_id = d.flow_id
    AND t.node = d2.parent
    AND t.is_node
  WHERE 
    d.parent = _parent
    AND d2.child = d.child
    AND d.flow_id = _flow_id
  GROUP BY 1, n.empty, f.only_these_nodes;
$$ LANGUAGE SQL;



/* push eligible tasks from dependency.
 *
 * This is a very special routine that runs inside the async orchestration
 * process, and is used to push tasks based on dependency processing.  Great
 * care must be taken for this routine not to fail, although errors are trapped,
 * it's good to be polite.
 */
CREATE OR REPLACE FUNCTION flow.task_complete() RETURNS TRIGGER AS 
$$
DECLARE
  _flow flow.flow;
  ft flow.v_flow_task;
  _last_step BOOL;
  _failed_step BOOL;
  _first_failure TEXT;
  
BEGIN
  SELECT INTO ft * FROM flow.v_flow_task WHERE task_id = new.task_id;

  /* look up the flow. if it's not there, we have bigger problems and will
   * punt.
   */
  SELECT INTO _flow * FROM flow.flow WHERE flow_id = ft.flow_id;

  IF NOT FOUND 
  THEN
    /* something or someone nuked the flow record.  Time to punt */
    PERFORM async.log(
      'WARNING',
      format(
        'Unable to find requeseted flow_id %s during task_complete trigger',
        ft.flow_id));
    RETURN NEW;
  END IF;

  IF _flow.processed IS NOT NULL
  THEN
    /* flow is considered to be finished in terms of processing, so, punt. */
    RETURN NEW;
  END IF;

  /* XXX: it may be better to verify parent node is still running before 
   * checking this for performance reasons.
   */
  IF NOT ft.is_node
  THEN
    PERFORM 1 FROM flow.v_flow_task t
    WHERE 
      flow_id = ft.flow_id
      AND node = ft.node
      AND NOT is_node
      AND processed IS NULL
    LIMIT 1;

    _last_step := NOT FOUND;
  END IF;

  IF NOT ft.is_node AND _last_step
  THEN
    IF (SELECT all_steps_must_complete FROM flow.node n WHERE n.node = ft.node)
    THEN
      _failed_step := EXISTS (
        SELECT 1 
        FROM flow.v_flow_task
        WHERE
          flow_id = ft.flow_id
          AND node = ft.node
          AND NOT is_node
          AND failed);
    ELSE 
      _failed_step := false;
    END IF;

    /* all steps mark finished. resolve the node which will then cascade 
     * dependency processing.  Node finish status is based this step status.
     */
    PERFORM async.finish_internal(
      array[t.task_id], 
      CASE WHEN _failed_step AND n.all_steps_must_complete 
        THEN 'FAILED'
        ELSE 'FINISHED'
      END::async.finish_status_t, 
      'task complete last step',
      CASE WHEN _failed_step AND n.all_steps_must_complete 
        THEN format('Steps did not complete from %s', ft.processing_error)
      END,
      NULL::INTERVAL)
    FROM flow.v_flow_task t
    JOIN flow.node n USING(node)
    WHERE
      flow_id = ft.flow_id
      AND node = ft.node
      AND processed IS NULL
      AND is_node;      
  ELSEIF NOT ft.is_node AND ft.failed 
  THEN
    /* node requires all steps to run and a step failed.  this means:
     * 1. halt any other running steps attached to this node. 
     * 2. marked other tasks as ineligible for processing
     */
    PERFORM async.finish_internal(
      array_agg(task_id),
      'FAILED'::async.finish_status_t,
      'task complete failed',
      format(
        'Failed due to failure of node %s step %s via %s', 
        ft.node, 
        ft.step_arguments,
        ft.processing_error),
      NULL::INTERVAL)
    FROM flow.v_flow_task t
    JOIN flow.node n ON 
      n.node = ft.node
      AND t.node = n.node
    WHERE 
      t.flow_id = ft.flow_id
      AND NOT is_node
      AND t.step_arguments != ft.step_arguments
      AND t.processed IS NULL
      AND n.all_steps_must_complete
    HAVING COUNT(*) > 0;
  ELSEIF ft.is_node
  THEN
    IF ft.failed
    THEN
      /* If there are any running steps underneath this node, fail them out
       * immediately.  This is unusual, since node completion would generally 
       * be based on steps completing.  Timeout and manual cancel would be 
       * reasons why this might occur.
       */
      PERFORM async.finish_internal(
        array_agg(task_id),
        'FAILED'::async.finish_status_t,
        'cascaded node failure',
        format(
          'Failed due to failure of node %s via %s', 
          ft.node, 
          ft.processing_error),
        NULL::INTERVAL)
      FROM flow.v_flow_task t
      JOIN flow.node n ON 
        n.node = ft.node
        AND t.node = n.node
      WHERE 
        t.flow_id = ft.flow_id
        AND NOT is_node
        AND t.processed IS NULL
      HAVING COUNT(*) > 0;
    END IF;

    /* The node did not have any steps (or did and failed), or this is the last
     * step to resolve. time to process node dependencies...introduce new task
     * if there are no other unmet dependencies.
     *
     * Upon failure, this is also a convenient time to push children with 
     * mandatory dependencies to parent out DOA, mainly so that if there is 
     * downstream 'run after errors' nodes, the dependency queue will trace down 
     * to them.  This can and will cascade to multiple trigger calls. However,
     * if the node is known to have no children with 'continue on failure'
     * set, dependencies will not be processed.
     */

    IF NOT ft.failed OR NOT (
      SELECT terminate_on_failure 
      FROM flow.flow_node fn
      WHERE 
        fn.flow = _flow.flow
        AND fn.node = ft.node)
    THEN
      PERFORM flow.push_tasks(
        ft.flow_id,
        array_agg(DISTINCT t),
        run_type,
        _source := format(
          'node complete (from task_id %s)',
          ft.task_id))
      FROM
      (
        SELECT 
          run_type,
          (child, '{}')::flow.task_wrapper_t t
        FROM flow.node_child_dependencies(ft.flow_id, ft.node)
        WHERE eligible
      ) q
      GROUP BY run_type;
    END IF;
  END IF;  
  
  RETURN new;
END;
$$ LANGUAGE PLPGSQL;

/* directly attaches to async table for dependency processing 
 * 
 * Trigger proceses task completion to cascade dependency effects 
 */
CREATE OR REPLACE TRIGGER on_flow_task_complete 
  AFTER INSERT OR UPDATE ON async.task 
  FOR EACH ROW WHEN (
    /* do not fire trigger unless... 
     *
     * ...task is completing and...
     */
    new.processed IS NOT NULL

    AND new.tracked IS DISTINCT FROM FALSE
    /* ...this is a task attached to flow  and ... */
    AND (new.task_data->>'flow_id') IS NOT NULL
    /* this is not a failure that itself is trigger fired */    
    AND new.finish_status IN ('FINISHED', 'FAILED', 'TIMED_OUT', 'DOA')
  )  
  EXECUTE PROCEDURE flow.task_complete();



/* directly attaches to async table for dependency processing 
 * 
 * Trigger intercepts node completion, if (and only if) it has steps, so that 
 * the node is not considered complete, but is yielded, while the steps are 
 * processing.  The last step completing (or node timeout) will then force the
 * node to complete.
 * 
 * For asynchronous nodes that have actual work to do, this is a 'second yield',
 * where the first is for the asynchronous processing response, the second is 
 * for pending steps.  To demystify this, the 'source' column is adjusted to 
 * note that yield is pending step completion.  Since this adjusts data en route
 * to the table, it's a before trigger which will also suppress the regular 
 * dependence processing trigger.
 */
CREATE OR REPLACE FUNCTION flow.check_node_steps() RETURNS TRIGGER AS
$$
DECLARE
  _flow_id INT DEFAULT (new.task_data->>'flow_id')::BIGINT;
  _node TEXT DEFAULT new.task_data->>'node';
  f flow.flow;
BEGIN
  IF OLD.source = 'step processing'
  THEN
    /* bail if task has already been yielded for step processing */
    RETURN new;
  END IF;

  /* if we are not running the complete flow, do not push anything if the node 
   * is not in the list of nodes to run.
   */
  SELECT INTO f * from flow.flow WHERE flow_id = _flow_id;

  IF f.only_these_nodes IS NOT NULL AND NOT _node = ANY(f.only_these_nodes)
  THEN
    RETURN new;
  END IF;

  /* to have gotten here, we know that the node is first completing via hacking, 
   * er, adjusting the source column to track having done this.  Only need to 
   * check node table to see if it has defined steps, or there are steps stashed
   * in the task itself via runtime configuration.  If neither are the case,
   * resolve without adjustment.
   */
  PERFORM flow.push_tasks(
    _flow_id,
    array_agg(t),
    _source := 'trigger create steps')
  FROM
  (
    SELECT 
      (
        _node,
        arguments
      )::flow.task_wrapper_t AS t
    FROM flow.step s
    WHERE s.node = _node
  ) q
  HAVING COUNT(*) > 0;

  IF EXISTS (
    SELECT 1 FROM flow.v_flow_task
    WHERE 
      flow_id = _flow_id
      AND node = _node
      AND NOT is_node)
  THEN
    /* yield the task for step processing */
    new.processed := NULL;
    new.yielded := clock_timestamp();
    new.failed := NULL;
    new.processing_error := NULL;
    new.finish_status = 'YIELDED';
    new.source := 'step processing';
  END IF;

  RETURN new;  
END;
$$ LANGUAGE PLPGSQL;

/*

CREATE OR REPLACE TRIGGER on_flow_check_node_steps
  BEFORE INSERT OR UPDATE ON async.task 
  FOR EACH ROW WHEN (
    /* do not fire trigger unless... 
     *
     * ...task is completing and...
     */
    new.processed IS NOT NULL

    AND new.tracked IS DISTINCT FROM FALSE

    /* ...this is a task attached to flow  and ... */
    AND (new.task_data->>'flow_id') IS NOT NULL

    /* ...is a node ... */
    AND flow.is_node(new.task_data->'step_arguments')

    /* this is not a failure that itself is trigger fired */    
    AND new.finish_status = 'FINISHED'
  )  
  EXECUTE PROCEDURE flow.check_node_steps();
*/


/* clean up processes when orchestator starts */
CREATE OR REPLACE PROCEDURE flow.async_startup() AS
$$
BEGIN
  PERFORM async.log('Performing flow cleanup');

  PERFORM flow.cancel(flow_id) 
  FROM flow.flow 
  WHERE flow.processed IS NULL;
END;
$$ LANGUAGE PLPGSQL;

/* mark flows finished */
CREATE OR REPLACE PROCEDURE flow.async_loop() AS
$$
DECLARE
  _finished BIGINT[];
BEGIN
  WITH f AS
  (
    UPDATE flow.flow SET processed = clock_timestamp() 
    WHERE flow_id IN (
      SELECT flow_id
      FROM
      (
        SELECT flow_id, 
          (
            SELECT task_id 
            FROM flow.v_flow_task t 
            WHERE 
              t.flow_id = f.flow_id 
              AND processed IS NULL LIMIT 1
          ) FROM flow.flow f WHERE processed IS NULL
      )
      WHERE task_id IS NULL
    )
    RETURNING flow_id
    
  )
  SELECT array_agg(flow_id) INTO _finished FROM f;

  IF array_upper(_finished, 1) >= 1
  THEN
    PERFORM async.log(
      format('Finishing flow_ids %s via flow complete', _finished));
  END IF; 
END;
$$ LANGUAGE PLPGSQL;

PERFORM async.push_routine('STARTUP', 'flow.async_startup()');
PERFORM async.push_routine('LOOP', 'flow.async_loop()');



CREATE OR REPLACE FUNCTION flow.configure_flow(
  _flow_name TEXT,
  _configuration JSONB,
  _append BOOL DEFAULT FALSE) RETURNS VOID AS
$$
DECLARE 
  r RECORD;
  _flow_configuration JSONB;

  _concurrency_group_routine TEXT 
    DEFAULT _configuration->>'concurrency_group_routine';
BEGIN
  /* Configure async module hooks */
  PERFORM async.push_routine('STARTUP', 'flow.async_startup()');
  PERFORM async.push_routine('LOOP', 'flow.async_loop()');

  CREATE TEMP TABLE tmp_flow (LIKE flow.flow_configuration) ON COMMIT DROP;
  CREATE TEMP TABLE tmp_node (LIKE flow.node) ON COMMIT DROP;

  ALTER TABLE tmp_node ADD COLUMN dependencies JSONB;
  ALTER TABLE tmp_node ADD COLUMN steps JSONB[];

  CREATE TEMP TABLE tmp_dependency (
    LIKE flow.dependency_configuration) ON COMMIT DROP;
  CREATE TEMP TABLE tmp_step (LIKE flow.step) ON COMMIT DROP;
  CREATE TEMP TABLE tmp_mutex (LIKE flow.mutex) ON COMMIT DROP;

  _flow_configuration := COALESCE(_configuration->'flow', '{}'::JSONB) ||
    format('{"flow": "%s"}', _flow_name)::JSONB;

  INSERT INTO tmp_flow SELECT * FROM jsonb_populate_record(
    null::tmp_flow,
    _flow_configuration);

  INSERT INTO tmp_node SELECT * FROM jsonb_populate_recordset(
    null::tmp_node,
    _configuration->'nodes');

  FOR r IN SELECT * FROM tmp_node
  LOOP
    INSERT INTO tmp_dependency
      SELECT (jsonb_populate_record(null::tmp_dependency, d)).*
      FROM 
      (
        SELECT to_jsonb(j) || format(
          '{"child": "%s", "flow": "%s"}',
          r.node,
          _flow_name)::JSONB AS d
        FROM jsonb_populate_recordset(
          null::tmp_dependency,
          r.dependencies) j
      ) q;  

    INSERT INTO tmp_step
      SELECT (jsonb_populate_record(null::tmp_step, s)).*
      FROM 
      (
        SELECT jsonb_build_object(
          'node', r.node,
          'arguments', s) AS s
        FROM unnest(r.steps) s
      ) q;
  END LOOP;

  INSERT INTO flow.flow_configuration SELECT * FROM tmp_flow
    ON CONFLICT DO NOTHING;

  UPDATE flow.flow_configuration SET 
    concurrency_group_routine = _concurrency_group_routine
  WHERE flow = _flow_name;

  INSERT INTO flow.node (node)
  SELECT t.node FROM tmp_node t
  WHERE node NOT IN (SELECT node FROM flow.node);

  INSERT INTO flow.flow_node
    SELECT _flow_name, node FROM tmp_node 
    ON CONFLICT DO NOTHING;

  UPDATE flow.node n SET
    target = COALESCE(t.target, n.target),
    priority = COALESCE(t.priority, n.priority),
    all_steps_must_complete = 
      COALESCE(t.all_steps_must_complete, n.all_steps_must_complete),
    empty = COALESCE(t.empty, n.empty),
    steps_to_flow = COALESCE(t.steps_to_flow, n.steps_to_flow),
    routine = t.routine,
    node_routine = t.node_routine,
    step_routine = t.step_routine,
    synchronous = COALESCE(t.synchronous, n.synchronous),
    node_timeout = t.node_timeout,
    step_timeout = t.step_timeout
  FROM tmp_node t
  WHERE t.node = n.node;

  INSERT INTO flow.dependency_configuration (flow, parent, child)
  SELECT flow, parent, child FROM tmp_dependency t
  WHERE (flow, parent, child) NOT IN (
    SELECT flow, parent, child FROM flow.dependency_configuration);

  UPDATE flow.dependency_configuration d SET
    continue_on_failure 
      = COALESCE(t.continue_on_failure, d.continue_on_failure)
  FROM tmp_dependency t
  WHERE (t.flow, t.parent, t.child) = (d.flow, d.parent, d.child);

  INSERT INTO flow.step SELECT * FROM tmp_step
    ON CONFLICT DO NOTHING;

  INSERT INTO flow.mutex SELECT * FROM tmp_mutex
    ON CONFLICT DO NOTHING;    

  IF NOT _append
  THEN
    DELETE FROM flow.flow_node
    WHERE flow = _flow_name AND node NOT IN (
      SELECT node from tmp_node);

    DELETE FROM flow.dependency_configuration 
    WHERE 
      flow = _flow_name
      AND (flow, parent, child) NOT IN (
        SELECT flow, parent, child FROM tmp_dependency);

    DELETE FROM flow.step 
    WHERE 
      (node, arguments) NOT IN (SELECT node, arguments FROM tmp_step)
      AND node IN (SELECT node FROM tmp_node);

    /* only clean up nodes if they are not attached to ANY flow */
    DELETE FROM flow.node WHERE node NOT IN (
      SELECT node FROM flow.flow_node);

  END IF;    

  /* optimize the optimizations! */
  PERFORM flow.set_terminate_on_failure(_flow_name);

  DROP TABLE tmp_flow;
  DROP TABLE tmp_node;
  DROP TABLE tmp_dependency;
  DROP TABLE tmp_step;
  DROP TABLE tmp_mutex;

END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION flow.configuration() RETURNS JSONB AS
$$
  SELECT jsonb_build_object(
    'flow', (SELECT jsonb_agg(f) FROM flow.flow_configuration f),
    'nodes', (SELECT jsonb_agg(n) FROM flow.node n),
    'steps', (SELECT jsonb_agg(s) FROM flow.step s),
    'dependencies', (SELECT jsonb_agg(d) FROM flow.dependency_configuration d),
    'mutexes', (SELECT jsonb_agg(m) FROM flow.mutex m));
$$ LANGUAGE SQL;



/* walks dependencies collecting facts as it goes. used to support configure
 * time processing, cylic checks, and other dependency analysis that will 
 * move processing from runtime to configure time.
 *
 * 'parents' means will walk from parents to children, while is appropriate 
 *    when doing depth checks, or anything that cascades state from top down.
 *
 *  anything requiring child up tracking, can't be directly returned from this
 *  function, for example, terminates_on_failure -- wrappers have to handle 
 *  that.
 */
CREATE OR REPLACE FUNCTION flow.walk_flow(
  _flow TEXT,
  parent OUT TEXT,
  child OUT TEXT,
  tree OUT TEXT[],
  continue_on_failure OUT BOOL, /* via dependency */
  depth OUT INT, /* depth to support cyclic checks */ 
  node OUT flow.node) RETURNS SETOF RECORD AS 
$$
  WITH RECURSIVE nodes AS
  ( 
    SELECT 
      dc.parent, 
      n.node AS child, 
      array[n.node] AS tree, 
      dc.continue_on_failure, 
      1 AS depth, 
      n 
    FROM flow.node n
    JOIN flow.flow_node fn USING(node)
    LEFT JOIN flow.dependency_configuration dc ON 
      n.node = dc.child
      AND dc.flow = fn.flow
    WHERE 
      fn.flow = _flow
      AND dc.parent IS NULL
    UNION ALL SELECT 
      dc.parent, 
      dc.child,
      nodes.tree || nc.node,
      dc.continue_on_failure,
      nodes.depth + 1,
      nc
    FROM nodes
    JOIN flow.dependency_configuration dc ON 
      dc.parent = nodes.child
      AND dc.flow = _flow
    JOIN flow.node nc ON nc.node = dc.child
    JOIN flow.flow_node fn USING(node, flow) 
    WHERE 
      fn.flow = _flow
      and nodes.depth <= 100
  )
  SELECT * FROM nodes;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION flow.set_terminate_on_failure(
  _flow TEXT) RETURNS VOID AS
$$
BEGIN
  UPDATE flow.flow_node fn SET terminate_on_failure = q.terminate_on_failure
  FROM
  (
    WITH w AS
    (
      SELECT * 
      FROM flow.walk_flow(_flow)
    )
    SELECT 
      fn.node,
      NOT COALESCE(bool_or(w.continue_on_failure OR EXISTS (
        SELECT 1
        FROM w
        WHERE 
          w.continue_on_failure
          AND fn.node = ANY(tree)
      )), false) AS terminate_on_failure
    FROM flow.flow_node fn
    JOIN w ON fn.node = w.child
    WHERE fn.flow = _flow
    GROUP BY fn.node
  ) q WHERE 
    fn.node = q.node
    AND fn.flow = _flow;
END;
$$ LANGUAGE PLPGSQL;

END;
$code$;