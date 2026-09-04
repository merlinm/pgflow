
/* Implements client side interfaces and and stanard structures for flow 
 * library. 
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
  RETURN;
EXCEPTION WHEN undefined_table THEN NULL;
END;

CREATE SCHEMA flow;

/* 
 * flow arguments are cached client side.
 */
CREATE TABLE flow.arguments
(
  flow_id BIGINT PRIMARY KEY,
  arguments JSONB
);

CREATE TYPE flow.callback_arguments_t AS
(
  flow_id BIGINT,
  flow TEXT,
  flow_arguments JSONB,
  node TEXT,
  step_arguments JSONB,
  task_id BIGINT
);

CREATE DOMAIN flow.flow_priority_t AS INT CHECK (value BETWEEN -99 AND 99);

END;
$bootstrap$;

DO
$code$
BEGIN

BEGIN
  PERFORM 1 FROM flow.arguments LIMIT 0;
EXCEPTION WHEN undefined_table THEN
  RAISE EXCEPTION 'Flow client library incorrectly installed';
  RETURN;
END;


/* 
 * Some nodes initialize steps on the fly.  To avoid race conditions they have 
 * to be pushed as any other task...the steps much be confirmed before the node
 * resolves.
 *
 * _flush_transaction_when_client: flow.finish() will attempt to flush 
 *    transaction changes so that the server does not race to push tasks 
 *    that may race to start before the calling transaction resolves.  This will
 *    not work if there is error handling outside of this proceure.  Suppressing
 *    the commit will allow for upper level error handling to occur. 
 */
CREATE OR REPLACE PROCEDURE flow.push_steps(
  _flow_id BIGINT,  
  _node TEXT,
  _arguments JSONB[],
  _flush_transaction_when_client BOOL DEFAULT TRUE) AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    IF _flush_transaction_when_client
    THEN
      COMMIT;
    END IF;

    PERFORM dblink_exec(
      async.server(), 
      format(
        'CALL flow.push_steps(%s, %s, %s)',
        quote_literal($1),
        quote_literal($2),
        quote_literal($3)));

    RETURN;
  END IF; 

  PERFORM flow.push_tasks(
    _flow_id,
   array_agg((_node, a)::flow.task_wrapper_t),
   _source := 'push steps')
  FROM unnest(_arguments) a;

  UPDATE flow.v_flow_task SET yield_upon_finish = true
  WHERE 
    flow_id = _flow_id
    AND node = _node 
    AND is_node
    AND processed IS NULL
    AND yield_upon_finish IS NOT TRUE
    AND array_upper(_arguments, 1) >= 1;

END;    
$$ LANGUAGE PLPGSQL;


/* Return arguments from the flow.  If they are not in the local cache, go get
 * them from the orchestrator.
 */
CREATE OR REPLACE FUNCTION flow.args(
  _flow_id BIGINT,
  args OUT JSONB) RETURNS JSONB AS
$$
DECLARE
  q TEXT;
BEGIN
  SELECT INTO args arguments
  FROM flow.arguments
  WHERE flow_id = _flow_id;

  IF FOUND
  THEN
    RETURN;
  END IF;

  IF (SELECT client_only FROM async.client_control)
  THEN
    SELECT INTO args * FROM dblink(
      async.server(), 
      format(
        'SELECT arguments FROM flow.flow WHERE flow_id = %s',
        _flow_id)) AS R(j JSONB);
  ELSE
    SELECT INTO args arguments
    FROM flow.flow 
    WHERE flow_id = _flow_id;
  END IF;

  INSERT INTO flow.arguments
  SELECT _flow_id, args
  ON CONFLICT DO NOTHING;
END;
$$ LANGUAGE PLPGSQL;

/* 
 * Will finish 'in-process' node. Useful when the node is set asynchronous, but
 * it is deterimined an asynchronous finish is not needed.
 */
CREATE OR REPLACE PROCEDURE flow.finish(
  _args flow.callback_arguments_t,
  _failed BOOL DEFAULT false,
  _error_message TEXT DEFAULT NULL,
  _flush_transaction_when_client BOOL DEFAULT true) AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    IF _flush_transaction_when_client
    THEN
      COMMIT;
    END IF;

    PERFORM dblink_exec(
      async.server(), 
      format(
        'CALL flow.finish(%s, %s, %s)',
        quote_literal($1),
        quote_literal($2),
        quote_nullable($3)));

    RETURN;
  END IF; 

  PERFORM async.finish(
    array[_args.task_id],
    CASE WHEN _failed THEN 'FAILED' ELSE 'FINISHED' END::async.finish_status_t,
      _error_message);
  
END;
$$ LANGUAGE PLPGSQL;

CREATE OR REPLACE PROCEDURE flow.defer(
  _args flow.callback_arguments_t,
  _duration INTERVAL) AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM dblink_exec(
      async.server(), 
      format(
        'CALL flow.defer(%s, %s)',
        quote_literal($1),
        quote_nullable($2)));

    RETURN;
  END IF; 

  PERFORM async.defer(
    array[_args.task_id],
   _duration);
END;
$$ LANGUAGE PLPGSQL;



/* Marks a flow and all attached tasks as ineligible to run. Any tasks
 * running synchronously will be cancelled.
 */
CREATE OR REPLACE FUNCTION flow.cancel(
  _flow_id BIGINT) RETURNS VOID AS
$$
DECLARE
  _task_ids BIGINT[];
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format('SELECT 0 FROM flow.cancel(%s)', $1)) AS R(V INT);

    RETURN;
  END IF; 

  PERFORM async.log(format('Canceling flow %s', _flow_id));

  SELECT INTO _task_ids array_agg(task_id) 
  FROM flow.v_flow_task 
  WHERE 
    flow_id = _flow_id
    AND processed IS NULL;

  UPDATE flow.flow SET processed = clock_timestamp()
  WHERE flow_id = _flow_id;

  IF array_upper(_task_ids, 1) >= 1
  THEN
    /* If all tasks are complete, flow need to be marked cancelled only. 
     *
     * Having no tasks to cancel should be quite rare in regular practice as 
     * the cancel would have to have lost the race to the last task finishing. 
     * More likely, an open flow with no extant tasks would be due to bad state 
     * management or external manipulation of the task table.
     */
     PERFORM async.cancel(_task_ids, 'flow cancel');
  END IF;

  /* cancel child flows (if any) */
  PERFORM flow.cancel(flow_id)
  FROM flow.flow
  WHERE 
    parent_flow_id = _flow_id
    AND processed IS NULL;
END;
$$ LANGUAGE PLPGSQL;



/* sets priority of a running flow. */
CREATE OR REPLACE FUNCTION flow.set_priority(
  _flow_id BIGINT,
  _priority flow.flow_priority_t) RETURNS VOID AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format('SELECT 0 FROM flow.set_priority(%s)', $1, $2)) AS R(V INT);

    RETURN;
  END IF; 

  /* do not prioritize flows that are finished */
  PERFORM 1 FROM flow.flow WHERE flow_id = _flow_id AND processed IS NULL;

  IF NOT FOUND
  THEN
    RETURN;
  END IF;

  UPDATE flow.flow SET force_priority = _priority
  WHERE 
    flow_id = _flow_id
    AND processed IS NULL
    AND force_priority != _priority;  

  /* adjust flow */
  UPDATE flow.v_flow_task SET priority = _priority 
  WHERE 
    flow_id = _flow_id
    AND processed IS NULL
    AND priority != _priority;

  /* adjust child flow */  
  UPDATE flow.v_flow_task t SET priority = _priority 
  FROM flow.flow f
  WHERE
    f.parent_flow_id = _flow_id
    AND t.flow_id = f.flow_id
    AND t.processed IS NULL
    AND t.priority != _priority;    
END;
$$ LANGUAGE PLPGSQL;


/* sets priority of a single step of a running flow.  If that step is configured
 * 'steps_to_flow', the attached flow will be prioritized as well.
 */
CREATE OR REPLACE FUNCTION flow.set_step_priority(
  _flow_id BIGINT,
  _task_id BIGINT,
  _priority flow.flow_priority_t) RETURNS VOID AS
$$

BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format('SELECT 0 FROM flow.set_step_priority(%s)', $1, $2, $3)) 
        AS R(V INT);
    RETURN;
  END IF; 

  /* XXX: only the orchestrator can directly adjust tasks */

  /* do not prioritize flows that are finished */
  PERFORM 1 FROM flow.flow WHERE flow_id = _flow_id AND processed IS NULL;

  IF NOT FOUND
  THEN
    RETURN;
  END IF;  

  UPDATE flow.v_flow_task SET priority = _priority 
  WHERE 
    flow_id = _flow_id
    AND task_id = _task_id
    AND processed IS NULL
    AND priority != _priority
    AND NOT is_node;

  /* reprioritize any child flows */
  PERFORM flow.set_priority(flow_id, _priority)
  FROM flow.flow
  WHERE 
    parent_flow_id = _flow_id
    AND parent_task_id = _task_id
    AND processed IS NULL;
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION flow.restart_flow(
  _flow_id BIGINT,
  _node TEXT DEFAULT NULL) RETURNS VOID AS
$$
DECLARE 
  _flow TEXT;
BEGIN
  UPDATE flow.flow SET processed = NULL
  WHERE processed IS NOT NULL AND flow_id = _flow_id
  RETURNING flow INTO _flow;

  IF NOT FOUND
  THEN
    PERFORM async.log(
      'ERROR', 
      format('Flow %s does not exist or is not finished', _flow_id));
  END IF;

  /* delete all tasks in flow that meet criteria */
  DELETE FROM flow.v_flow_task t
  WHERE 
    flow_id = _flow_id
    AND
    (
      _node IS NULL
      OR
      (
        t.node IN (
          SELECT child 
          FROM flow.walk_flow(_flow) 
          WHERE 
            tree @> array[_node]
            AND 
            (
              /* XXX: static steps should not be deleted */
              step_arguments != '{}'
              OR child != _node
            )
        )
      )
    );

  PERFORM async.restart_task(task_id)
  FROM flow.v_flow_task
  WHERE 
    flow_id = _flow_id
    AND node = _node;

END;
$$ LANGUAGE PLPGSQL;

END;
$code$;




CREATE OR REPLACE FUNCTION flow.create_flow(
  _flow TEXT,
  _arguments JSONB,
  _only_these_nodes TEXT[] DEFAULT NULL, 
  _add_parents BOOL DEFAULT FALSE, /* XXX: not implemented */
  _add_children BOOL DEFAULT FALSE, /* XXX: not implemented */
  _parent_task_id BIGINT DEFAULT NULL,
  _force_priority INT DEFAULT NULL, /* run all nodes at this priority */
  flow_id OUT BIGINT) RETURNS BIGINT AS
$$
BEGIN
  IF (SELECT client_only FROM async.client_control)
  THEN
    PERFORM * FROM dblink(
      async.server(), 
      format(
        'SELECT 0 FROM flow.create_flow(%s, %s, %s, %s, %s, %s, %s)', 
        quote_literal($1), 
        quote_literal($2), 
        quote_nullable($3), 
        quote_nullable($4), 
        quote_nullable($5), 
        quote_nullable($6), 
        quote_nullable($7))) 
        AS R(V INT);
    RETURN;
  END IF; 

  INSERT INTO flow.flow(flow, 
    arguments, parent_task_id, only_these_nodes, parent_flow_id, force_priority)
  SELECT 
    _flow,
    _arguments,
    _parent_task_id,
    _only_these_nodes,
    t.flow_id,
    /* force priority can be directly specfified, or inherited from the 
     * creating parent step if there is one.
     */
    COALESCE(_force_priority, t.priority)
  FROM flow.flow_configuration
  LEFT JOIN flow.v_flow_task t ON
    t.task_id = _parent_task_id
  WHERE flow = _flow
  RETURNING flow.flow_id INTO create_flow.flow_id;

  IF NOT FOUND THEN
    /* not server logged intentionally since this is a end user invokable 
     * routine.
     */
    RAISE EXCEPTION 'Missing flow configuration %', _flow;
  END IF;

  /* Copy the dependency configuration into the flow_id based instance, so that
   * dependences 'as processed' are preserved. 
   */
  INSERT INTO flow.dependency SELECT 
    create_flow.flow_id,
    parent,
    child,
    continue_on_failure
  FROM flow.dependency_configuration dc
  WHERE dc.flow = _flow;

  /* Push task from nodes that have no parent */
  PERFORM flow.push_tasks(
    create_flow.flow_id,
    array_agg((node, '{}')::flow.task_wrapper_t),
    CASE 
      WHEN empty THEN 'EMPTY' 
      WHEN _only_these_nodes IS NULL THEN 'EXECUTE'
      WHEN node = ANY(_only_these_nodes) THEN 'EXECUTE'
      ELSE 'EMPTY'
    END::async.task_run_type_t,
    'create flow')
  FROM
  (
    SELECT 
      fn.node,
      n.empty
    FROM flow.flow_node fn
    JOIN flow.node n USING(node)
    WHERE 
      fn.flow = _flow
      AND NOT EXISTS (
        SELECT 1 FROM flow.dependency d2
        WHERE 
          d2.flow_id = create_flow.flow_id
          AND d2.child = fn.node
      )
  ) q
  GROUP BY CASE 
      WHEN empty THEN 'EMPTY' 
      WHEN _only_these_nodes IS NULL THEN 'EXECUTE'
      WHEN node = ANY(_only_these_nodes) THEN 'EXECUTE'
      ELSE 'EMPTY'
    END::async.task_run_type_t;

  /* do we actually have to do anything? */
  IF NOT EXISTS (
    SELECT 1 FROM flow.v_flow_task t
    WHERE t.flow_id = create_flow.flow_id)
  THEN
    PERFORM async.log(
      'WARNING', 
      format(
        'Auto finishing empty flow %s',
        create_flow.flow_id));

    UPDATE flow.flow f SET processed = clock_timestamp()
    WHERE f.flow_id = create_flow.flow_id;
  END IF;
END;
$$ LANGUAGE PLPGSQL;


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
/* Views and functions to support flow administration from UI */



/* get flow data and status */
CREATE OR REPLACE VIEW flow.v_flow_node_status AS
  SELECT 
    f.flow_id,
    f.flow,
    n.node,
    n2.target,
    t.consumed AS started,
    t.processed AS finished,
    CASE 
      WHEN t.finish_status = 'FINISHED' THEN 'Finished'
      WHEN t.processed IS NOT NULL THEN 'Failed'
      WHEN f.processed IS NOT NULL AND t.Consumed IS NULL THEN 'Cancelled'
      WHEN t.task_id IS NULL THEN 'Pending'
      WHEN t.yielded IS NOT NULL 
        AND COUNT(*) FILTER (WHERE step_id IS NOT NULL) > 0 THEN 'Running Steps'
      WHEN t.yielded IS NOT NULL THEN 'Running Async'
      WHEN t.consumed IS NOT NULL THEN 'Running'
      ELSE 'Unknown'
    END AS status,
    COUNT(*) FILTER (WHERE s.step_id IS NOT NULL) AS steps,
    COUNT(*) FILTER (WHERE s.Status = 'Pending') AS steps_pending,
    COUNT(*) FILTER (WHERE s.Status IN ('Running', 'Running Async')) AS steps_running,
    COUNT(*) FILTER (WHERE s.Status = 'Finished') AS steps_finished,
    COUNT(*) FILTER (WHERE s.Status = 'Failed') AS steps_failed,
    all_steps_must_complete,
    CASE WHEN t.consumed IS NULL 
      THEN ''
      ELSE async.interval_pretty(coalesce(t.processed, now()) - t.consumed) 
    END AS run_time,
    greatest(t.consumed, t.processed, t.yielded) AS changed,
    t.consumed IS NOT NULL AND t.processed IS NULL AS in_progress    
  FROM flow.flow f
  JOIN flow.flow_node n USING(flow)
  JOIN flow.node n2 USING(node)
  LEFT JOIN flow.v_flow_task t ON 
    f.flow_id = t.flow_id
    AND n.node = t.node
    AND flow.is_node(t.step_arguments)
  LEFT JOIN 
  (
    SELECT 
      t.flow_id,
      t.task_id AS step_id,
      t.node AS node,
      CASE 
        WHEN t.consumed IS NULL THEN 'Pending'
        WHEN t.processed IS NULL AND t.asynchronous_finish THEN 'Running Async'
        WHEN t.processed IS NULL THEN 'Running'
        WHEN t.finish_status = 'FINISHED' THEN 'Finished'
        WHEN t.processed IS NOT NULL THEN 'Failed'
        ELSE 'Unknown'
      END AS status
    FROM flow.v_flow_task t 
    WHERE NOT flow.is_node(t.step_arguments)
  ) s ON 
    s.flow_id = f.flow_id   
    AND s.node = n.node
  GROUP BY 1,2,3, n2.target, t.finish_status, t.processed, t.yielded, 
    t.consumed, t.task_id, all_steps_must_complete;


CREATE OR REPLACE VIEW flow.v_flow_task_status AS
  SELECT 
    f.flow_id,
    t.task_id,
    CASE WHEN flow.is_node(t.step_arguments) THEN 'Node' ELSE 'Step' END AS Type,
    f.flow,
    n.node,
    n2.target,
    to_char(t.consumed, 'YYYY-MM-DD HH:MI:SS') AS started,
    CASE WHEN t.Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    async.interval_pretty(COALESCE(t.processed, now()) - t.consumed) AS run_time,
    CASE 
      WHEN t.finish_status = 'FINISHED' THEN 'Finished'
      WHEN t.processed IS NOT NULL THEN 'Failed'
      WHEN f.processed IS NOT NULL AND t.Consumed IS NULL THEN 'Cancelled'
      WHEN t.task_id IS NULL THEN 'Pending'
      WHEN t.consumed IS NULL THEN 'Pending'
      WHEN t.yielded IS NOT NULL 
        AND is_node THEN 'Running Steps'
      WHEN t.yielded IS NOT NULL THEN 'Running Async'
      WHEN t.consumed IS NOT NULL THEN 'Running'
      ELSE 'Unknown'
    END AS status,
    step_arguments::TEXT AS step_arguments,
    t.processing_error,
    replace(t.query, '##flow.TASK_ID##', t.task_id::TEXT) AS query
  FROM flow.flow f
  JOIN flow.flow_node n USING(flow)
  JOIN flow.node n2 USING(node)
  LEFT JOIN flow.v_flow_task t ON 
    f.flow_id = t.flow_id
    AND n.node = t.node;


  

CREATE OR REPLACE VIEW flow.v_flow_status_internal AS
  SELECT
    f.flow_id,
    f.flow,
    CASE WHEN f.Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    async.interval_pretty(COALESCE(f.processed, now()) - f.created) AS run_time,
    COUNT(*) AS count_nodes,
    COUNT(*) FILTER (WHERE NOT t.Failed) AS count_finished_nodes,
    COUNT(*) FILTER (WHERE t.Failed 
      OR (t.task_id IS NULL AND f.Processed IS NOT NULL)) AS count_failed_nodes,
    f.arguments,
    any_value(processing_error ORDER BY t.Processed) 
      FILTER (
        WHERE 
          failed 
          AND processing_error NOT LIKE 'Failed due to%'
          AND length(processing_error) > 0
        )
      AS first_error
  FROM flow.flow f
  LEFT JOIN flow.flow_node n USING(flow)
  LEFT JOIN flow.v_flow_task t ON
    f.flow_id = t.flow_id
    AND n.node = t.node 
    AND flow.is_node(t.step_arguments)
  GROUP BY 1,2;

CREATE OR REPLACE VIEW flow.v_flow_status AS
  SELECT
    f.flow_id,
    f.flow,
    CASE WHEN f.Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    async.interval_pretty(COALESCE(f.processed, now()) - f.created) AS run_time,
    count_nodes::BIGINT,
    count_finished_nodes::BIGINT,
    count_failed_nodes::BIGINT,
    f.arguments,
    first_error
  FROM flow.flow f
  WHERE count_nodes IS NOT NULL
  UNION ALL SELECT
    flow_id,
    flow,
    CASE WHEN Processed IS NULL THEN 'No' ELSE 'Yes' END AS complete,
    async.interval_pretty(COALESCE(processed, now()) - created) AS run_time,
    (i->>'count_nodes')::BIGINT,
    (i->>'count_finished_nodes')::BIGINT,
    (i->>'count_failed_nodes')::BIGINT,
    arguments,
    i->>'first_error'
  FROM 
  (
    SELECT 
      f.*, 
      (
        SELECT to_json(i)
        FROM flow.v_flow_status_internal i
        WHERE flow_id = f.flow_id      
      ) i
    FROM flow.flow f
    WHERE count_nodes IS NULL
  ) q;

/* list of flows and their configuration */
DROP VIEW IF EXISTS flow.v_flow_configuration;
CREATE OR REPLACE VIEW flow.v_flow_configuration AS
  SELECT 
    fc.flow,
    concurrency_group_routine,
    f.created AS last_execution_time,
    CASE 
      WHEN f.processed IS NULL THEN NULL
      WHEN f.count_failed_nodes = 0 THEN true
      ELSE false
    END AS last_execution_success,
    'RUN' AS flow_concurrency_control,  /* RUN / QUEUE / BLOCK */
    f.flow_id AS last_execution_flow_id,
    fl.count_flows_running::INT,
    0 AS count_flows_pending,
    fs.first_error AS last_error_message
  FROM flow.flow_configuration fc
  LEFT JOIN
  (
    SELECT 
      flow, 
      max(flow_id) AS flow_id,
      count(*) FILTER(WHERE processed IS NULL) AS count_flows_running
    FROM flow.flow
    GROUP BY 1
  ) fl USING(flow)
  LEFT JOIN flow.flow f USING(flow_id)
  LEFT JOIN flow.v_flow_status fs USING(flow_id);




CREATE OR REPLACE FUNCTION flow.flows(
  _limit INT DEFAULT 1000) RETURNS JSONB AS
$$
  SELECT jsonb_agg(s)
  FROM 
  (
    SELECT * 
    FROM flow.v_flow_status 
    ORDER BY flow_id DESC
    LIMIT _limit
  ) s;
$$ LANGUAGE SQL;

CREATE OR REPLACE FUNCTION flow.task_list(_flow_id BIGINT) RETURNS JSONB AS
$$
  SELECT jsonb_agg(t)
  FROM 
  (
    SELECT * FROM flow.v_flow_task_status 
    WHERE 
      flow_id = _flow_id
      ORDER BY task_id DESC
  ) t;
$$ LANGUAGE SQL;




CREATE OR REPLACE FUNCTION flow.graphviz_node(
  _node TEXT,
  _target TEXT,
  _steps_overview TEXT,
  _runtime TEXT,
  _color TEXT,
  _pending BOOL,
  cell OUT TEXT) RETURNS TEXT AS
$$
DECLARE
  _cols TEXT;
BEGIN
  SELECT INTO _cols
    format('<TR><TD id="%s.target" BGCOLOR="%s">%s</TD></TR>', _node, _color, _target);

  SELECT INTO cell format($s$
    "%s" [id=%s
  label=<
    <TABLE BORDER="0" CELLBORDER="1" CELLSPACING="0" CELLPADDING="4">
    <TR><TD COLSPAN="1">%s</TD></TR>
    %s
    %s %s
    </TABLE>> shape=none  ];
    $s$,
    replace(_node, '.', '_'),
    replace(_node, '.', '_'),
    _node,
    _cols ,
    CASE WHEN _steps_overview != ''
      THEN format(
        '<TR><TD id="%s.steps" COLSPAN="1">steps: %s</TD></TR>', 
        _node, 
        _steps_overview)
      WHEN _pending THEN 
        format(
          '<TR><TD id="%s.steps" COLSPAN="1">steps pending</TD></TR>',
          _node)
      ELSE 
        format(
          '<TR><TD id="%s.steps" COLSPAN="1">no steps</TD></TR>',
          _node)
    END,
    CASE WHEN _runtime != ''
      THEN format('<TR><TD id="%s.runtime" COLSPAN="1">%s</TD></TR>',
        _node, 
        _runtime)
      ELSE ''
    END);
  END ;
$$ LANGUAGE PLPGSQL;



CREATE OR REPLACE FUNCTION flow.node_status_color(
  _status TEXT) RETURNS TEXT AS
$$
  SELECT CASE _status
    WHEN 'Pending' THEN 'beige'
    WHEN 'Running Async' THEN 'orange'
    WHEN 'Running Steps' THEN 'orange'
    WHEN 'Running' THEN 'orange'
    WHEN 'Finished' THEN 'palegreen'
    WHEN 'Cancelled' THEN 'indianred'
    WHEN 'Failed' THEN 'indianred'
    ELSE 'beige'
  END;
$$ LANGUAGE SQL IMMUTABLE;


CREATE OR REPLACE FUNCTION flow.steps_overview(
  s flow.v_flow_node_status) RETURNS TEXT AS
$$
  SELECT CASE 
    WHEN s.steps > 0
    THEN
      format(
        'pending %s running %s finished %s failed %s',
        s.steps_pending,
        s.steps_running,
        s.steps_finished,
        s.steps_failed)
    ELSE ''
  END;
$$ LANGUAGE SQL IMMUTABLE;



CREATE OR REPLACE FUNCTION flow.graphviz(
  _flow_id BIGINT, Data OUT TEXT) RETURNS TEXT AS
$$
DECLARE
  _Tree TEXT;
  _Format TEXT;
  f flow.flow;

BEGIN
  SELECT INTO f * FROM flow.flow WHERE flow_id = _flow_id;

  WITH d AS
  (
    SELECT DISTINCT
      n.node,
      coalesce(p.parent, 'start') AS parent,
      coalesce(c.child, 'end') AS child,
      target,
      color,
      p.continue_on_failure,
      steps_overview,
      run_time,
      status,
      NOT all_steps_must_complete AND steps_overview != '' AS partial_steps
    FROM
    (
      SELECT
        flow_id,
        node,
        target,
        s.run_time,
        flow.node_status_color(status) AS color,
        flow.steps_overview(s) AS steps_overview,
        status,
        all_steps_must_complete
      FROM flow.v_flow_node_status s
    ) n
    LEFT JOIN
    (
      SELECT d.flow_id, parent, child, continue_on_failure
      FROM flow.dependency d
      LEFT JOIN flow.v_flow_task t ON
        d.flow_id = t.flow_id
        AND d.parent = t.node
        AND flow.is_node(t.step_arguments)
    ) p ON n.node = p.child AND n.flow_id = p.flow_id
    LEFT JOIN
    (
      SELECT d.flow_id, parent, child, continue_on_failure
      FROM flow.dependency d
      LEFT JOIN flow.v_flow_task t ON
        d.flow_id = t.flow_id
        AND d.child = t.node      
        AND flow.is_node(t.step_arguments)
    ) c ON n.node = c.parent AND n.flow_id = c.flow_id
    WHERE n.flow_id = _flow_id
  )
  SELECT INTO _Tree, _Format
    string_agg(
      format('%s -> %s%s',
        replace(parent, '.', '_'),
        replace(node, '.', '_'),
        CASE 
          WHEN continue_on_failure AND partial_steps
            THEN ' [arrowhead=empty, dir=both, arrowtail = "invempty"]'
          WHEN continue_on_failure AND NOT partial_steps
            THEN ' [arrowhead=empty]'
          WHEN NOT continue_on_failure AND partial_steps
            THEN ' [dir=both, arrowtail = "invempty"]'
          ELSE ''
        END), E'\n'
        ORDER BY parent, node),
    string_agg(
      DISTINCT flow.graphviz_node(
        node,
        target,
        steps_overview,
        run_time ,
        color,
        status = 'Pending') , E'\n')
        FILTER(WHERE node != 'end')
  FROM
  (
    SELECT DISTINCT 
      parent, node, target, color, continue_on_failure, steps_overview, run_time, status, partial_steps
    FROM d 
    UNION ALL SELECT DISTINCT 
      node, 'end', target, color, false, steps_overview, run_time, status, partial_steps
    FROM d
    WHERE Child = 'end'
  ) q;

  SELECT INTO Data format($q$
digraph "%s" {
  edge [arrowsize="1.5"]

  %s%s%s

  start [shape=invtriangle label="%s"];
  end [shape=triangle label = "END"];

  legend  [shape=record];
  flow_pending [label = "Pending" shape="rectangle" style="striped" fillcolor = "beige"];
  flow_running [label = "Running" shape="rectangle"  style="striped" fillcolor = "orange"];
  flow_finished [label = "Finished" shape="rectangle"  style="striped" fillcolor = "palegreen"];
  flow_failed [label = "Failed" shape="rectangle"  style="striped" fillcolor = "indianred"];

  legend->flow_pending [arrowhead=empty label="  run on fail"];
  flow_pending->flow_running [label="  fail on fail"];
  flow_running->flow_finished ;
  flow_running->flow_failed [label="steps may fail", dir=both, arrowtail = "invempty"];

}$q$,
  format('%s/%s', f.flow, f.flow_id),
  _Tree,
  E'\n',
  _Format,
  format(E'%s\nid: %s', f.flow, f.flow_id));
END;
$$ LANGUAGE PLPGSQL;


CREATE OR REPLACE FUNCTION flow.graphviz_events(
  _flow_id BIGINT,
  _since TIMESTAMPTZ, 
  node OUT TEXT,
  node_status_color OUT TEXT,
  node_steps_overview OUT TEXT,
  run_time OUT TEXT) RETURNS SETOF RECORD AS
$$
  SELECT 
    node,
    flow.node_status_color(status),
    flow.steps_overview(s),
    run_time
  FROM flow.v_flow_node_status s
  WHERE
    flow_id = _flow_id 
    AND (changed > _since OR in_progress OR _since IS NULL)
  ORDER BY changed
$$ LANGUAGE SQL;




